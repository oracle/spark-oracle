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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions._
import org.apache.spark.sql.oracle.operators.{
  OraJoinClause,
  OraQueryBlock,
  OraSingleQueryBlock,
  OraTableScan
}

case class JoinPushdown(
    inDSScan: DataSourceV2ScanRelation,
    leftOraScan: OraScan,
    leftQBlk: OraQueryBlock,
    rightQBlk: OraQueryBlock,
    pushdownCatalystOp: Join,
    joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinCond: Option[Expression],
    sparkSession: SparkSession)
    extends OraPushdown
    with PredicateHelper {

  import JoinPushdown._

  override val inOraScan: OraScan = leftOraScan
  override val inQBlk: OraQueryBlock = leftQBlk

  private def buildJoinQBlock(
      joinOp: Join,
      rightProjList: Seq[OraExpression],
      oraCond: Option[OraExpression]): OraQueryBlock = {
    (joinCond, joinType, rightQBlk) match {
      case CollapsibleJoin(oraTabScan) =>
        val oraJoin = OraJoinClause(joinType, oraTabScan, oraCond)
        val newWhere = {
          val filts = currQBlk.where.toSeq ++ rightQBlk.where.toSeq
          filts match {
            case Nil => None
            case _ =>
              Some(filts.reduceLeft[OraExpression] {
                case (oE, cond) =>
                  OraBinaryOpExpression(AND, And(oE.catalystExpr, cond.catalystExpr), oE, cond)
              })
          }
        }
        currQBlk.copyBlock(
          select = currQBlk.select ++ rightProjList,
          where = newWhere,
          joins = currQBlk.joins :+ oraJoin,
          catalystOp = Some(joinOp),
          catalystProjectList = joinOp.output)
      case _ =>
        val oraJoin = OraJoinClause(joinType, rightQBlk, oraCond)
        currQBlk.copyBlock(
          select = currQBlk.select ++ rightProjList,
          joins = currQBlk.joins :+ oraJoin,
          catalystOp = Some(joinOp),
          catalystProjectList = joinOp.output)
    }
  }

  private def pushEquiJoin: Option[OraQueryBlock] = {
    val joinOp = pushdownCatalystOp
    for (leftOraExprs <- OraExpressions.unapplySeq(leftKeys);
         rightOraExprs <- OraExpressions.unapplySeq(rightKeys);
         rightProjList <- OraExpressions.unapplySeq(rightQBlk.catalystAttributes);
         oraJoinCond = joinCond.flatMap(jC => OraExpression.unapply(jC));
         if (joinCond.isDefined == oraJoinCond.isDefined)) yield {
      val oraEqConds = leftOraExprs.zip(rightOraExprs).map {
        case (l, r) => OraBinaryOpExpression(EQ, EqualTo(l.catalystExpr, r.catalystExpr), l, r)
      }
      val oraConds = oraEqConds ++ oraJoinCond.toSeq
      val oraCond = oraConds.reduceLeft[OraExpression] {
        case (oE, cond) =>
          OraBinaryOpExpression(AND, And(oE.catalystExpr, cond.catalystExpr), oE, cond)
      }
      buildJoinQBlock(joinOp, rightProjList, Some(oraCond))
    }
  }

  private def pushCrossJoin: Option[OraQueryBlock] = {
    val joinOp = pushdownCatalystOp
    for (rightProjList <- OraExpressions.unapplySeq(rightQBlk.catalystAttributes)) yield {
      buildJoinQBlock(joinOp, rightProjList, None)
    }
  }

  override private[rules] def pushdownSQL: Option[OraQueryBlock] = {

    if (currQBlk.canApply(pushdownCatalystOp)) {
      if (leftKeys.nonEmpty) {
        pushEquiJoin
      } else if (!joinCond.isDefined) {
        pushCrossJoin
      } else None
    } else None
  }
}

object JoinPushdown {

  /**
   * Attempt to collapse joining [[OraQueryBlock]] into left [[OraQueryBlock]].
   *
   * Conservatively this is ok when inner joining a block that is a TableScan with no
   * computed projections.
   *
   * Since the current right [[OraQueryBlock]] has different
   * [[ExprId]]s, setup the [[OraTableScan]] that is joined in to have the current
   * query block's `catalystAttributes` and `catalystOp`.
   *
   */
  object CollapsibleJoin {

    def unapply(arg: Any): Option[OraTableScan] = arg match {
      /*
        - can also allow case (RightOuter, OraTableScan)
          - in this case applying filter of right attributes after join is
            semantically equivalent, since right side is not null producing.
       */
      case (None, Inner | Cross, qBlk: OraSingleQueryBlock) =>
        import qBlk._
        val canCollapse: Boolean = {
          if (!hasComputedShape && !hasJoins && !hasAggregate) {
            source match {
              case oTbl: OraTableScan => true
              case _ => false
            }
          } else false
        }

        if (canCollapse) {

          /**
           * This OraTableScan was wrapped into a QueryBlock in
           * [[OraSQLPushdownRule.toOraQueryBlock]].
           * So just return it here.
           */
          Some(qBlk.source.asInstanceOf[OraTableScan])
        } else None
      case _ => None
    }
  }

}
