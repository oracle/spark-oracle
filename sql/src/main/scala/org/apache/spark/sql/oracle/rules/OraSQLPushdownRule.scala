/*
  Copyright (c) 2021, Oracle and/or its affiliates.

  This software is dual-licensed to you under the Universal Permissive License
  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
  either license.

  If you elect to accept the software under the Apache License, Version 2.0,
  the following applies:

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
package org.apache.spark.sql.oracle.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.read.oracle.{OraPushdownScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.oracle.expressions.{
  AND,
  OraBinaryOpExpression,
  OraExpression,
  OraExpressions
}
import org.apache.spark.sql.oracle.operators.{
  OraPlan,
  OraQueryBlock,
  OraSingleQueryBlock,
  OraTableScan
}

object OraSQLPushdownRule extends OraLogicalRule with Logging {

  /**
   * Setting up a [[OraQueryBlock]] for a [[OraTableScan]]:
   *
   *  - the [[OraExpression]]s in the [[OraTableScan]] refer to
   * [[org.apache.spark.sql.catalyst.expressions.AttributeReference spark attrs]] that have
   * different [[org.apache.spark.sql.catalyst.expressions.ExprId expr_ids]] from
   * the once in the [[DataSourceV2ScanRelation#output dsv2 output]].
   *  - we fix this by setting up `OraQBlk <- QraTableScan` tree with
   *    the expr_ids of the output and expressions matching the dsv2.output expr_ids.
   *
   * @param oraPlan
   * @param dsv2
   * @return
   */
  private def toOraQueryBlock(oraPlan: OraPlan, dsv2: DataSourceV2ScanRelation): OraQueryBlock =
    oraPlan match {
      case oraTScan: OraTableScan =>
        val oraProjs = OraExpressions.unapplySeq(dsv2.output).get
        val fils = (oraTScan.filter.toSeq ++ oraTScan.partitionFilter.toSeq)
        val oraFil = if (fils.nonEmpty) {
          Some(fils.reduceLeft[OraExpression] {
            case (l, r) => OraBinaryOpExpression(AND, l.catalystExpr, l, r)
          })
        } else None

        OraSingleQueryBlock(
          oraTScan.copy(
            catalystOp = Some(dsv2),
            catalystProjectList = dsv2.output,
            projections = Seq.empty,
            filter = None,
            partitionFilter = None),
          Seq.empty, None, oraProjs, oraFil, None, Some(dsv2), dsv2.output, None
        )
      case oraQBlck: OraQueryBlock => oraQBlck
    }

  private def pushProjectFilters(
      dsV2: DataSourceV2ScanRelation,
      scanPlan: LogicalPlan,
      projections: Seq[NamedExpression],
      filters: Seq[Expression])(implicit sparkSession: SparkSession): LogicalPlan = {

    def getLastProjectFilter(
        endplan: LogicalPlan,
        startPlan: LogicalPlan,
        hasProjects: Boolean,
        hasFilters: Boolean): Seq[LogicalPlan] = {
      var p = endplan
      var prjFnd = !hasProjects
      var filFnd = !hasFilters
      var s = Seq.empty[LogicalPlan]
      while ((!prjFnd || !filFnd) && (p != startPlan)) {
        p match {
          case pr: Project if !prjFnd =>
            s = pr +: s
            prjFnd = true
          case fl: Filter if !filFnd =>
            s = fl +: s
            filFnd = true
          case _ => ()
        }
        p = p.children.head
      }
      s
    }

    def withOraQBlock(dsv2: DataSourceV2ScanRelation): DataSourceV2ScanRelation = {
      val oraScan: OraScan = dsv2.scan.asInstanceOf[OraScan]
      val oraQBlk: OraQueryBlock = toOraQueryBlock(oraScan.oraPlan, dsv2)
      dsv2.copy(scan = OraPushdownScan(oraScan.sparkSession, oraScan.dsKey, oraQBlk))
    }

    val pushPlans = getLastProjectFilter(scanPlan, dsV2, projections.nonEmpty, filters.nonEmpty)
    /*
     * set the [[OraPlan]] in the [[DataSourceV2ScanRelation]] as a [[OraQueryBlock]]
     */
    val dsV2WithOraQBlock = withOraQBlock(dsV2)

    val pushedDSV2 =
      pushPlans.foldLeft(Some(dsV2WithOraQBlock): Option[DataSourceV2ScanRelation]) {
        case (None, _) => None
        case (Some(dsV2 @ DataSourceV2ScanRelation(_, oraScan: OraScan, _)), p: Project) =>
          ProjectPushdown(
            dsV2,
            oraScan: OraScan,
            oraScan.oraPlan.asInstanceOf[OraQueryBlock],
            p,
            sparkSession: SparkSession).pushdown
        case (Some(dsV2 @ DataSourceV2ScanRelation(_, oraScan: OraScan, _)), f: Filter) =>
          FilterPushdown(
            dsV2,
            oraScan: OraScan,
            oraScan.oraPlan.asInstanceOf[OraQueryBlock],
            f,
            sparkSession: SparkSession).pushdown
        case _ => None
      }

    pushedDSV2.getOrElse(scanPlan)
  }

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan = {

    /* First collapse Project-Filter by transformDown */

    val plan1 = plan transformDown {
      case scanPlan @ PhysicalOperation(projections, filters, dsV2: DataSourceV2ScanRelation) =>
        pushProjectFilters(dsV2, scanPlan, projections, filters)
    }

    plan1 transformUp {
      case scanPlan @ PhysicalOperation(projections, filters, dsV2: DataSourceV2ScanRelation) =>
        pushProjectFilters(dsV2, scanPlan, projections, filters)
      case joinOp @ ExtractEquiJoinKeys(
            joinType,
            leftKeys,
            rightKeys,
            condition,
            leftChild @ DataSourceV2ScanRelation(_, oraScanL: OraScan, _),
            rightChild @ DataSourceV2ScanRelation(_, oraScanR: OraScan, _),
            _) =>
        joinType match {
          /*
             - for example tpcds q10, q35
             - TODO: how to handle exists boolean attribute on top of the join operator
           */
          case _: ExistenceJoin => joinOp
          case LeftAnti | LeftSemi =>
            SemiAntiJoinPushDown(
              leftChild,
              oraScanL,
              toOraQueryBlock(oraScanL.oraPlan, leftChild),
              toOraQueryBlock(oraScanR.oraPlan, rightChild),
              joinOp,
              joinType,
              leftKeys,
              rightKeys,
              condition,
              sparkSession).pushdown.getOrElse(joinOp)
          case _ =>
            JoinPushdown(
              leftChild,
              oraScanL,
              toOraQueryBlock(oraScanL.oraPlan, leftChild),
              toOraQueryBlock(oraScanR.oraPlan, rightChild),
              joinOp,
              joinType,
              leftKeys,
              rightKeys,
              condition,
              sparkSession).pushdown.getOrElse(joinOp)
        }
      case joinOp @ Join(
            leftChild @ DataSourceV2ScanRelation(_, oraScanL: OraScan, _),
            rightChild @ DataSourceV2ScanRelation(_, oraScanR: OraScan, _),
            Inner | Cross,
            None,
            _) =>
        JoinPushdown(
          leftChild,
          oraScanL,
          toOraQueryBlock(oraScanL.oraPlan, leftChild),
          toOraQueryBlock(oraScanR.oraPlan, rightChild),
          joinOp,
          joinOp.joinType,
          Seq.empty,
          Seq.empty,
          None,
          sparkSession).pushdown.getOrElse(joinOp)
      // in case of a LeftAnti, ExtractEquiJoin doesn't pattern match
      // NotInJoinPattern. See note in NotInJoinPattern.scala
      case joinOp @ Join(
            leftChild @ DataSourceV2ScanRelation(_, oraScanL: OraScan, _),
            rightChild @ DataSourceV2ScanRelation(_, oraScanR: OraScan, _),
            LeftAnti,
            Some(condition),
            _) =>
        SemiAntiJoinPushDown(
          leftChild,
          oraScanL,
          toOraQueryBlock(oraScanL.oraPlan, leftChild),
          toOraQueryBlock(oraScanR.oraPlan, rightChild),
          joinOp,
          LeftAnti,
          Seq.empty,
          Seq.empty,
          Some(condition),
          sparkSession).pushdown.getOrElse(joinOp)
      case aggOp @ Aggregate(_, _, child @ DataSourceV2ScanRelation(_, oraScan: OraScan, _)) =>
        AggregatePushdown(
          child,
          oraScan,
          toOraQueryBlock(oraScan.oraPlan, child),
          aggOp,
          sparkSession).pushdown.getOrElse(aggOp)
      case expOp @ Expand(_, _, child @ DataSourceV2ScanRelation(_, oraScan: OraScan, _)) =>
        ExpandPushdown(
          child,
          oraScan,
          toOraQueryBlock(oraScan.oraPlan, child),
          expOp,
          sparkSession).pushdown.getOrElse(expOp)
      case gl @ GlobalLimit(_,
            LocalLimit(_, dsV2 @ DataSourceV2ScanRelation(_, oraScan: OraScan, _))) =>
        LimitPushdown(
          dsV2,
          oraScan,
          toOraQueryBlock(oraScan.oraPlan, dsV2),
          gl,
          sparkSession).pushdown.getOrElse(gl)
      case sort @ Sort(_, global, child @
        DataSourceV2ScanRelation(_, oraScan: OraScan, _)) if global =>
        OrderByPushDown(child,
          oraScan,
          toOraQueryBlock(oraScan.oraPlan, child),
          sort,
          sparkSession).pushdown.getOrElse(sort)
      case window @ Window(_, _, _, child @
        DataSourceV2ScanRelation(_, oraScan: OraScan, _)) =>
        WindowPushDown(child,
          oraScan,
          toOraQueryBlock(oraScan.oraPlan, child),
          window,
          sparkSession
        ).pushdown.getOrElse(window)
      case u @ Union(OraScans(childScans @ _*), false, false) =>
        import org.apache.spark.sql.oracle.OraSQLImplicits._
        val (dsv2s, oraScans) = childScans.unzip
        val childOraPlans = childScans map {
          case (dsV2, oraScan) => toOraQueryBlock(oraScan.oraPlan, dsV2)
        }
        SetOpPushdown(dsv2s.head, oraScans.head, childOraPlans,
          u, osql"UNION ALL", sparkSession
        ).pushdown.getOrElse(u)
      case e @ Except(
            leftChild @ DataSourceV2ScanRelation(_, oraScanL: OraScan, _),
            rightChild @ DataSourceV2ScanRelation(_, oraScanR: OraScan, _),
            false) =>
        import org.apache.spark.sql.oracle.OraSQLImplicits._
        SetOpPushdown(leftChild, oraScanL,
          Seq(toOraQueryBlock(oraScanL.oraPlan, leftChild),
            toOraQueryBlock(oraScanR.oraPlan, rightChild)),
          e, osql"MINUS", sparkSession
        ).pushdown.getOrElse(e)
      case i @ Intersect(
            leftChild @ DataSourceV2ScanRelation(_, oraScanL: OraScan, _),
            rightChild @ DataSourceV2ScanRelation(_, oraScanR: OraScan, _),
            false) =>
        import org.apache.spark.sql.oracle.OraSQLImplicits._
        SetOpPushdown(leftChild, oraScanL,
          Seq(toOraQueryBlock(oraScanL.oraPlan, leftChild),
            toOraQueryBlock(oraScanR.oraPlan, rightChild)),
          i, osql"INTERSECT", sparkSession
        ).pushdown.getOrElse(i)
    }
  }

  object OraScans {
    def unapplySeq(plans: Seq[LogicalPlan]): Option[Seq[(DataSourceV2ScanRelation, OraScan)]] =
      OraSparkUtils.sequence(plans map {
        case dsv2 @ DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
          Some((dsv2, oraScan)).asInstanceOf[Option[(DataSourceV2ScanRelation, OraScan)]]
        case _ => None
      }
      )
  }
}
