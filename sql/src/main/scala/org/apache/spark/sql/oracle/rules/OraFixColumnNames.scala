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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions.{Named, OraExpression}
import org.apache.spark.sql.oracle.expressions.Named._
import org.apache.spark.sql.oracle.expressions.Subquery.OraSubqueryExpression
import org.apache.spark.sql.oracle.operators.{OraCompositeQueryBlock, OraPlan, OraSingleQueryBlock, OraTableScan}

/**
 * Ensure 'correct' column names used in oracle-sql. This entails 3 things:
 *  - where possible use the catalog name of a column instead of the case insensitive
 *    name used in Spark.
 *  - When there are multiple
 *    [[org.apache.spark.sql.catalyst.expressions.AttributeReference attributes]]
 *    with the same name apply de-dup strategies of qualifying names or generating
 *    new names.
 *  - If Spark name exceed 30 characters applying truncation.
 *
 * <img src="doc-files/fixNames.png" />
 *
 * '''Column Name case:'''
 *
 * In ''Spark SQL'' the default and preferred behavior is that of '''case insensitive'''
 * resolution of column names. Spark documenation says:
 * 'highly discouraged to turn on case sensitive mode.'
 * This means that if you have a column defined with the mixed case name 'aA', in
 * Spark SQL it is ok to refer to it as aa or AA... Further the optimized Logical Plan
 * will carry an [[AttributeReference]] with that name(aa or AA..) used in the sql statement.
 *
 * Whereas in oracle sql unquoted names are treated to mean upper-case names. So unquoted
 * names such as aa or AA .. used in a sql statement are resolved to mean the column 'AA'.
 *
 * So when generating oracle sql from a Spark Optimized Logical Plan we need to
 * walk down the Spark Plan to find the Catalog column that an [[AttributeReference]]
 * is for and use that name.
 *
 * '''Column Qualification:'''
 *
 * In [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan Optimized Spark Plans]] columns
 * are identified by [[org.apache.spark.sql.catalyst.expressions.ExprId expr_id]]; so it is
 * possible to end up with Operator shapes that contain multiple columns with the
 * same name. When generating `Oracle SQL` from these plans we need to disambiguate
 * duplicate names.
 *
 * We will operate on a [[OraPlan translated oracle plan]] that is output from the
 * [[OraSQLPushdownRule]] and apply the following disambiguation logic.
 *  - if there are multiple attributes with the same name among the inputs
 *    of a [[OraQueryBlock]], we disambiguate these attributes by qualifying each
 *    by the input they are from. Inputs to the QueryBlock are assigned ''qualifiers''.
 *  - if the output of a [[OraPlan]] have multiple attributes with the same name.
 *    We generate new aliases form duplicate attributes.
 *
 * '''Example:'''
 *
 * In the following query `C_INT` is a column in 4 table references. The optimized spark plan
 * contains the [[org.apache.spark.sql.catalyst.expressions.AttributeReference]] `C_INT`
 * for all of them. With the disambiguated oracle-sql is also listed below. `C_INT`
 * references in the top query block are qualified and in the top select the projections
 * have new aliases associated.
 * {{{
 * // SQL
 * select a.c_int, b.c_int, c.c_int, d.c_int
 * from sparktest.unit_test a,
 *      sparktest.unit_test_partitioned b,
 *      sparktest.unit_test c,
 *      sparktest.unit_test_partitioned d
 * where a.c_int = b.c_int and b.c_int = c.c_int and c.c_int = d.c_int
 *
 * // Spark optimized plan without oracle pushdown
 * !Join Inner, (c_int#46 = c_int#63)
 * !:- Join Inner, (c_int#27 = c_int#46)
 * !:  :- Join Inner, (c_int#10 = c_int#27)
 * !:  :  :- Filter isnotnull(C_INT#10)
 * !:  :  :  +- RelationV2[C_INT#10] SPARKTEST.UNIT_TEST
 * !:  :  +- Filter isnotnull(C_INT#27)
 * !:  :     +- RelationV2[C_INT#27] SPARKTEST.UNIT_TEST_PARTITIONED
 * !:  +- Filter isnotnull(C_INT#46)
 * !:     +- RelationV2[C_INT#46] SPARKTEST.UNIT_TEST
 * !+- Filter isnotnull(C_INT#63)
 * !   +- RelationV2[C_INT#63] SPARKTEST.UNIT_TEST_PARTITIONED
 *
 * // oracle-sql generated
 * SELECT "sparkora_0"."C_INT" AS "C_INT_1_sparkora",
 *        "sparkora_1"."C_INT" AS "C_INT_2_sparkora",
 *        "sparkora_2"."C_INT" AS "C_INT_3_sparkora",
 *        "sparkora_3"."C_INT" AS "C_INT_4_sparkora"
 * FROM SPARKTEST.UNIT_TEST "sparkora_0"
 * JOIN SPARKTEST.UNIT_TEST_PARTITIONED "sparkora_1"
 *          ON ("sparkora_0"."C_INT" = "sparkora_1"."C_INT")
 * JOIN SPARKTEST.UNIT_TEST "sparkora_2"
 *          ON ("sparkora_1"."C_INT" = "sparkora_2"."C_INT")
 * JOIN SPARKTEST.UNIT_TEST_PARTITIONED "sparkora_3"
 *          ON ("sparkora_2"."C_INT" = "sparkora_3"."C_INT")
 * WHERE ((("sparkora_0"."C_INT" IS NOT NULL
 *          AND "sparkora_1"."C_INT" IS NOT NULL)
 *         AND "sparkora_2"."C_INT" IS NOT NULL)
 *        AND "sparkora_3"."C_INT" IS NOT NULL)
 * }}}
 */
object OraFixColumnNames extends OraLogicalRule with Logging {

  val ORA_FIXED_NAMES_TAG = TreeNodeTag[Boolean]("fixedOraColumnNames")

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan =
    plan transformUp {
      case dsv2@DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
        val oraPlan = oraScan.oraPlan
        val namesFixed = oraPlan.getTagValue(ORA_FIXED_NAMES_TAG).getOrElse(false)
        if (!namesFixed) {
          fixNames(oraPlan)
          oraPlan.setTagValue(ORA_FIXED_NAMES_TAG, true)
        }
        dsv2
    }

  def fixNames(oraPlan: OraPlan): Unit =
    FixPlan(oraPlan, 0).execute

  private type SourcePos = Int
  private val NAME_TAG = "sparkora"
  private val ORA_NM_MAX_SZ = 30

  private def fixOraNm(nm : String, i : Int) : (Int, String) = {
    if (nm.size > ORA_NM_MAX_SZ || replaceNm(nm)) {
      val j = i + 1
      (j, genNm(nm, j))
    } else (i, nm)
  }

  private val internalNmPattern = "\\.|#|CAST".r

  private def replaceNm(nm : String) : Boolean =
    internalNmPattern.findFirstIn(nm).isDefined

  private def genNm(nm : String, i : Int) : String = {
    val iNm = s"${i}_${NAME_TAG}"
    if (replaceNm(nm)) {
      iNm
    } else {
      val p = nm.substring(0, Math.min(nm.length(), ORA_NM_MAX_SZ - iNm.length - 1))
      s"${p}_${iNm}"
    }
  }

  sealed trait FixPlan {
    def oraPlan: OraPlan
    def projectList : Seq[OraExpression]
    def numSources: Int
    def pos: SourcePos
    lazy val qualifier: String = s"${NAME_TAG}_${pos}"
    def source(id: SourcePos): FixPlan
    def colRefsInCorrSubQs : Set[ExprId]

    case class QualCol(srcPos: SourcePos, name: String) {
      lazy val fixedName = QualFixedColNm(source(srcPos).qualifier, name)
    }

    case class InputDetails(inEIdMap : Map[ExprId, QualCol],
                            oraNamesInScope : Map[SourcePos, Set[String]]) {
      val dupNames: Map[String, Seq[ExprId]] = {
        (for ((eId, qNm) <- inEIdMap.toSeq) yield {
          (qNm.name, eId)
        }).groupBy(_._1).
          filter(t => t._2.size > 1).
          mapValues(s => s.map(_._2))
      }

      val dupNamesinScope : Set[String] = {
        val nmPosMap : Seq[(String, SourcePos)] = (for (
          (srcPos, names) <- oraNamesInScope.iterator;
          name <- names.iterator) yield {
          (name, srcPos)
        }).toSeq

        nmPosMap.
          groupBy(t => t._1).
          filter(t => t._2.size > 1).
          keySet
      }

      val exprIdsFromDupNamesInScope : Set[ExprId] = {
        (for ((eId, qNm) <- inEIdMap.toSeq if (dupNamesinScope.contains(qNm.name))) yield {
          eId
        }).toSet
      }

      val qualifiedExprIdsForDupNames = dupNames.values.flatten.toSet

      val qualifiedExprIds =
        qualifiedExprIdsForDupNames ++ exprIdsFromDupNamesInScope ++ colRefsInCorrSubQs

      val qualifiedInputs : Set[SourcePos] = inEIdMap.filter {
        case (eId, _) => qualifiedExprIds.contains(eId)
      }.map(t => t._2.srcPos).toSet

      /*
       * capture 'fixed' Lateral Join output names
       */
      val fixedLJOut : Map[ExprId, String] = {
        val ab = ArrayBuffer[(ExprId, String)]()
        /*
         * why start at inEIdMap.size?
         * - because there can be name clashes between the input up-to the
         *   lateral join and lateral-join projections.
         * - for expressions in grouping sets Spark generates a new ExprId in
         *   Expand.out. So we retain a OraLatJoinProjEntry for them.
         *   Then the fixNm logic could end by generating the same name.
         *   (happens for cube test on `d_year + 1`
         *
         * why generate names?
         * - expand projections are mostly column references of the input
         * - since lateral join in oracle sql includes left(input) projections
         *   most of the time we get name conflicits.
         *   - so for now we just introduce new names for expand projections.
         */
        var i : Int = inEIdMap.size
        for ( (eId, a) <- latJoinOutMap.iterator) {
          val nm = genNm(a.name, i)
          i = i + 1
          ab += ((eId, nm))
        }
        ab.toMap
      }

      /*
       * When fixing query-block names we want to
       * apply the lat Join renames for expressions
       * after the lateral Join, but not for expressions
       * before. So apply the rewrite on expressions
       * in the where, select and group-by clauses.
       */
      def fixName(oc: OraColumnRef, applyLJFixes : Boolean): Unit = {
        val exprId = oc.catalystExpr.exprId
        if (qualifiedExprIds.contains(exprId)) {
          oc.setOraFixedNm(inEIdMap(exprId).fixedName)
        } else {
          if (inEIdMap.contains(exprId)) {
            val inNm = inEIdMap(exprId).name
            if (inNm != oc.outNmInOraSQL) {
              oc.setOraFixedNm(UnQualFixedColNm(inNm))
            }
          } else if (applyLJFixes && fixedLJOut.contains(exprId) ) {
            val inNm = fixedLJOut(exprId)
            if (inNm != oc.outNmInOraSQL) {
              oc.setOraFixedNm(UnQualFixedColNm(inNm))
            }
          }
        }
      }
    }

    def latJoinOutMap : Map[ExprId, Attribute]

    lazy val inDetails = {
      val inEIdMap : Map[ExprId, QualCol] = (
          for (srcPos <- (0 until numSources))  yield {
          source(srcPos).outEIdMap.mapValues(s => QualCol(srcPos, s))
          }
        ).reduceLeft(_ ++ _)

      val oraNamesInScope = (
        for (srcPos <- (0 until numSources))  yield {
          (srcPos, source(srcPos).extraOraNamesInScope)
        }
      ).toMap

      InputDetails(inEIdMap, oraNamesInScope)
    }

    protected def fixInternals : Unit

    def outEIdMap : Map[ExprId, String]

    /*
     * when we translate to ora-sql, we translate
     * an [[OraTableScan]] into a table reference;
     * this means that all its columns are in scope in query block
     * it is contained in. So name de-dupilcation must account for this.
     */
    def extraOraNamesInScope : Set[String]

    def fixProjectList : Unit = projectList.foreach {
      case oNE : OraNamedExpression =>
        val outNm = outEIdMap(oNE.catalystExpr.exprId)
        if (oNE.outNmInOraSQL != outNm) {
          oNE.setOraFixedAlias(outNm)
        }
      case _ => ()
    }

    /**
     * Ensure attribute references in order by use the output name of this query block.
     * So references in generated-sql matches the aliases in the select list.
     */
    protected def fixOrderBy : Unit

    def execute : Unit = {
      fixInternals
      fixProjectList
      fixOrderBy
    }
  }

  case class TableScanFixPlan(oraPlan : OraTableScan,
                              pos: SourcePos) extends FixPlan {

    override def projectList: Seq[OraExpression] = oraPlan.projections
    override def numSources: SourcePos = 0
    override def source(id: SourcePos): FixPlan = null
    override def colRefsInCorrSubQs : Set[ExprId] = Set.empty

    lazy val latJoinOutMap : Map[ExprId, Attribute] = Map.empty

    override protected def fixInternals : Unit = ()

    override protected def fixOrderBy : Unit = ()

    override def outEIdMap: Map[ExprId, String] =
      oraPlan.catalystAttributes.map(a => a.exprId -> a.name).toMap

    override def extraOraNamesInScope : Set[String] =
      oraPlan.oraTable.columns.map(_.name).toSet
  }

  case class QBlkFixPlan(oraPlan : OraSingleQueryBlock,
                         pos: SourcePos) extends FixPlan {
    val projectList: Seq[OraExpression] = oraPlan.select
    val numSources: SourcePos = 1 + oraPlan.joins.size

    val childPlansMap: Map[SourcePos, FixPlan] = {
      val childPlans = FixPlan(oraPlan.source, 0) +:
        (for ((jn, i) <- oraPlan.joins.zipWithIndex) yield {
          FixPlan(jn.joinSrc, i + 1)
        })

      (for (cP <- childPlans) yield {
        cP.execute
        (cP.pos, cP)
      }).toMap
    }

    lazy val latJoinOutMap : Map[ExprId, Attribute] =
      oraPlan.latJoin.map(_.expand.output.map(a => a.exprId -> a).toMap).
        getOrElse(Map.empty)

    case class OutputDetails(projectList : Seq[OraExpression]) {
      /*
       * In Sub-queries the projectList entry may not be OraNamedExpression.
       * We can ignore these; the sub-query predicate's output doesn't apply to its
       * outer query
       */
      val eIdMap : Map[ExprId, String] = (projectList.collect {
        case oNE : OraNamedExpression => oNE.catalystExpr.exprId -> oNE.outNmInOraSQL
      }).toMap

      val dupNames: Map[String, Seq[ExprId]] = {
        (for ((eId, nm) <- eIdMap.toSeq) yield {
          (nm, eId)
        }).groupBy(_._1).
          filter(t => t._2.size > 1).
          mapValues(s => s.map(_._2))
      }

      val dupExprIds = dupNames.values.flatten.toSet

      /*
       * Fix names that are duplicates or have more than
       * 30 characters.
       */
      val outEIdMap : Map[ExprId, String] = {
        var i: Int = 0
        for((eId, nm) <- eIdMap) yield {
          val oNm = if (dupExprIds.contains(eId)) {
            i += 1
            genNm(nm, i)
          } else {
            val (j, oNm) = fixOraNm(nm, i)
            i = j
            oNm
          }
          eId -> oNm
        }
      }
    }

    lazy val outDetails = OutputDetails(projectList)
    lazy val outEIdMap : Map[ExprId, String] = outDetails.outEIdMap
    lazy val extraOraNamesInScope : Set[String] = Set.empty

    override def source(id: SourcePos): FixPlan = childPlansMap(id)

    override def colRefsInCorrSubQs : Set[ExprId] = {
      (for (outRef <- Named.outerRefs(oraPlan)) yield {
        outRef.oraColRef.catalystExpr.exprId
      }).toSet
    }

    override protected def fixInternals: Unit = {
      def fixOE(oE : OraExpression, applyLJFixes : Boolean) : Unit = {
        oE.foreachUp {
          case oc : OraColumnRef => inDetails.fixName(oc, applyLJFixes)
          case _ => ()
        }
      }

      def fixSubQueryExprs(oE : OraExpression) : Unit = {
        val subQueries = oE.collect {
          case oSE : OraSubqueryExpression => oSE.oraPlan
        }
        for(sQ <- subQueries) {
          OraFixColumnNames.fixNames(sQ)
        }
      }

      /* 0. fix subquery expressions */
      for (oE <- oraPlan.where) {
        fixSubQueryExprs(oE)
      }
      for(oE <- oraPlan.select) {
        fixSubQueryExprs(oE)
      }

      /* 1. source and joins */
      if (inDetails.qualifiedInputs.contains(0)) {
        val child = childPlansMap(0)
        oraPlan.setSourceAlias(child.qualifier)
      }

      for ((jc, i) <- oraPlan.joins.zipWithIndex) yield {
        val childPos = i + 1
        if (inDetails.qualifiedInputs.contains(childPos)) {
          val child = childPlansMap(childPos)
          jc.setJoinAlias(child.qualifier)
        }
        if (jc.onCondition.isDefined) {
          fixOE(jc.onCondition.get, false)
        }
      }

      /* latJoin */
      if (oraPlan.latJoin.isDefined) {
        fixLatJoinAliases(inDetails.fixedLJOut)
        oraPlan.latJoin.get.foreach(oE => fixOE(oE, false))
      }

      /* select */
      oraPlan.select.foreach(oE => fixOE(oE, true))

      /* where */
      oraPlan.where.foreach(oE => fixOE(oE, true))

      /* fix outer references in subquery expression */
      Named.outerRefs(oraPlan).map(oE => fixOE(oE.oraColRef, true))

      /* groupBys */
      oraPlan.groupBy.foreach(gBys => gBys.foreach(oE => fixOE(oE, true)))

      /* orderBys */
      oraPlan.orderBy.foreach(oBys => oBys.foreach(oE => fixOE(oE, true)))

    }

    /**
     * Ensure attribute references in order by use the output name of this query block.
     * So references in generated-sql matches the aliases in the select list.
     */
    override protected def fixOrderBy : Unit = {
      for (sortOEs <- oraPlan.orderBy) {
        for (
          sE <- sortOEs;
          oE <- sE
        ) {
          oE match {
            case oNE: OraColumnRef =>
              val outNm = outEIdMap(oNE.catalystExpr.exprId)
              if (oNE.outNmInOraSQL != outNm) {
                oNE.setOraFixedNm(UnQualFixedColNm(outNm))
              }
            case _ => ()
          }
        }
      }
    }

    def fixLatJoinAliases(ljOutMap : Map[ExprId, String]) : Unit = {
      if (oraPlan.latJoin.isDefined) {
        val headPL = oraPlan.latJoin.get.projections.head
        for(ljPE <- headPL.projectList if ljPE.outAttr.isDefined) {
          val outAttr = ljPE.outAttr.get
          val outNm = ljOutMap(outAttr.exprId)
          if (outNm != outAttr.name) {
            ljPE.setOraFixedAlias(outNm)
          }
        }
      }
    }
  }

  case class CompQBlkFixPlan(oraPlan : OraCompositeQueryBlock,
                             pos: SourcePos) extends FixPlan {

    /*
     * oracle sql you are allowed to have:
     * select "C_INT" AS "val"
     * ....
     * union all
     *  select ("C_INT" + "C_LONG") AS "1_sparkora"
     * ...
     * (See SetOpTranslationTest.union1 test)
     *
     * So no need to ensure childPlans have the same column names/aliases.
     */

    val childPlans : Seq[FixPlan] = oraPlan.children.map { oPlan =>
        val cPlan = FixPlan(oPlan, 0)
        cPlan.execute
        cPlan
    }

    val firstChild = childPlans.head

    override def projectList: Seq[OraExpression] = firstChild.projectList

    override def numSources: SourcePos = firstChild.numSources

    override def source(id: SourcePos): FixPlan = null

    override def colRefsInCorrSubQs : Set[ExprId] = Set.empty

    override def latJoinOutMap: Map[ExprId, Attribute] = Map.empty

    override protected def fixInternals: Unit = ()

    override protected def fixOrderBy : Unit = ()

    override def outEIdMap: Map[ExprId, String] = firstChild.outEIdMap

    override def extraOraNamesInScope: Set[String] = Set.empty
  }

  object FixPlan {
    def apply(oraPlan: OraPlan,
              pos: SourcePos) : FixPlan = oraPlan match {
      case oT : OraTableScan => TableScanFixPlan(oT, pos)
      case qBlk : OraSingleQueryBlock => QBlkFixPlan(qBlk, pos)
      case cQBlk : OraCompositeQueryBlock => CompQBlkFixPlan(cQBlk, pos)
      case _ => null
    }
  }
}
