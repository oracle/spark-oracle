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
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.{OraSparkUtils, SQLSnippet}
import org.apache.spark.sql.oracle.expressions.{AND, Named, OraBinaryOpExpression, OraExpression, OraExpressions, OraLiteral, OraLiteralSql}
import org.apache.spark.sql.oracle.expressions.Subquery.OraSubQueryJoin
import org.apache.spark.sql.oracle.operators.OraQueryBlock

/**
 * '''Left Semi Joins:'''
 *
 * `in subquery` and `exists correlated subquery` predicates in Spark SQL
 * get translated into a `Left Semi-Join` operation.
 * We translate these into `in subquery` when generating Oracle SQL.
 *
 * ''Example 1:''
 * For the Spark SQL query:
 * {{{
 * select c_long
 * from sparktest.unit_test
 * where c_int in (select c_int
 *                 from sparktest.unit_test_partitioned
 *                 where c_long = sparktest.unit_test.c_long
 *                 )
 * }}}
 * the Spark Plan is:
 * {{{
 * !Project [C_LONG#13L]
 * !+- Join LeftSemi, ((C_INT#12 = C_INT#29) AND (c_long#30L = C_LONG#13L))
 * !   :- RelationV2[C_INT#12, C_LONG#13L] SPARKTEST.UNIT_TEST
 * !   +- RelationV2[C_INT#29, C_LONG#30L] SPARKTEST.UNIT_TEST_PARTITIONED
 * }}}
 * This gets translated to the following Oracle SQL:
 * {{{
 * select "C_LONG"
 * from SPARKTEST.UNIT_TEST
 * where  ("C_INT", "C_LONG") in
 *    ( select "C_INT", "C_LONG" from SPARKTEST.UNIT_TEST_PARTITIONED )
 * }}}
 *
 * ''Example 2:''
 * For the Spark SQL query:
 * {{{
 * with ssales as
 * (select ss_item_sk
 * from store_sales
 * where ss_customer_sk = 8
 * ),
 * ssales_other as
 * (select ss_item_sk
 * from store_sales
 * where ss_customer_sk = 10
 * )
 * select ss_item_sk
 * from ssales
 * where exists (select ssales_other.ss_item_sk
 *               from ssales_other
 *               where ssales_other.ss_item_sk = ssales.ss_item_sk
 *               )
 * }}}
 * the Spark Plan is:
 * {{{
 * !Join LeftSemi, (ss_item_sk#183 = SS_ITEM_SK#160)
 * !:- Project [SS_ITEM_SK#160]
 * !:  +- Filter (isnotnull(SS_CUSTOMER_SK#161) AND (SS_CUSTOMER_SK#161 = 8.000000000000000000))
 * !:     +- RelationV2[SS_ITEM_SK#160, SS_CUSTOMER_SK#161] TPCDS.STORE_SALES
 * !+- RelationV2[SS_ITEM_SK#183] TPCDS.STORE_SALES
 * }}}
 * This gets translated to the following Oracle SQL:
 * {{{
 * select "SS_ITEM_SK"
 * from TPCDS.STORE_SALES
 * where (("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) AND  "SS_ITEM_SK" in ( select "SS_ITEM_SK"
 * from TPCDS.STORE_SALES
 * where ("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) ))
 * }}}
 *
 * '''Left Anti Joins (not in):'''
 *
 * `not in subquery`  predicates in Spark SQL
 * get translated into a `Left Anti-Join` operation with a
 * 'is null' equality conjunction. See [[NotInJoinPattern]] for more details.
 * We translate these into `not in subquery` when generating Oracle SQL.
 *
 * For the Spark SQL query:
 * {{{
 * select c_long
 * from sparktest.unit_test
 * where c_int not in (select c_int
 *                 from sparktest.unit_test_partitioned
 *                 where c_long = sparktest.unit_test.c_long
 *                 )
 * }}}
 * the Spark Plan is:
 * {{{
 * !Project [C_LONG#309L]
 * !+- Join LeftAnti, (((C_INT#308 = C_INT#325) OR isnull((C_INT#308 = C_INT#325))) AND (c_long#326L = C_LONG#309L))
 * !   :- RelationV2[C_INT#308, C_LONG#309L] SPARKTEST.UNIT_TEST
 * !   +- RelationV2[C_INT#325, C_LONG#326L] SPARKTEST.UNIT_TEST_PARTITIONED
 * }}}
 * - the join condition on the [[LeftAnti]] join outputs `unit_test` rows with null 'c_int' values.
 * This gets translated to the following Oracle SQL:
 * {{{
 * select "sparkora_0"."C_LONG"
 * from SPARKTEST.UNIT_TEST "sparkora_0"
 * where  "C_INT" NOT IN ( select "C_INT"
 *                         from SPARKTEST.UNIT_TEST_PARTITIONED
 *                         where ("sparkora_0"."C_LONG" = "C_LONG")
 *                       )
 * }}}
 * - we translate into an oracle sql not in subquery and rely on no-in smenatics in Oracle.
 *
 * '''Left Anti Joins (not exists):'''
 * For the Spark SQL query:
 * {{{
 * select c_long
 * from sparktest.unit_test
 * where not exists (select c_int
 *                 from sparktest.unit_test_partitioned
 *                 where c_long = sparktest.unit_test.c_long and
 *                       c_int = sparktest.unit_test.c_int
 *                 )
 * }}}
 * the Spark Plan is:
 * {{{
 * !Project [C_LONG#187L]
 * !+- Join LeftAnti, ((c_long#204L = C_LONG#187L) AND (c_int#203 = C_INT#186))
 * !   :- RelationV2[C_INT#186, C_LONG#187L] SPARKTEST.UNIT_TEST
 * !   +- RelationV2[C_INT#203, C_LONG#204L] SPARKTEST.UNIT_TEST_PARTITIONED
 * }}}
 * This gets translated to the following Oracle SQL:
 * {{{
 *   select "sparkora_0"."C_LONG"
 *   from SPARKTEST.UNIT_TEST "sparkora_0"
 *   where not exists ( select 1
 *                      from SPARKTEST.UNIT_TEST_PARTITIONED
 *                      where (("sparkora_0"."C_LONG" = "C_LONG") AND ("sparkora_0"."C_INT" = "C_INT"))
 *                    )
 * }}}
 *
 * @param inDSScan
 * @param leftOraScan
 * @param leftQBlk
 * @param rightQBlk
 * @param pushdownCatalystOp
 * @param joinType
 * @param leftKeys
 * @param rightKeys
 * @param joinCond
 * @param sparkSession
 */
case class SemiAntiJoinPushDown(inDSScan: DataSourceV2ScanRelation,
                                leftOraScan: OraScan,
                                leftQBlk: OraQueryBlock,
                                rightQBlk: OraQueryBlock,
                                pushdownCatalystOp: Join,
                                joinType: JoinType,
                                leftKeys: Seq[Expression],
                                rightKeys: Seq[Expression],
                                joinCond: Option[Expression],
                                sparkSession: SparkSession
                               )
  extends OraPushdown with PredicateHelper {
  override val inOraScan: OraScan = leftOraScan
  override val inQBlk: OraQueryBlock = leftQBlk

  private def dereference(exps : Seq[Expression],
                         oraQBlock : OraQueryBlock) : Seq[Expression] = {
    val aliasMap = getAliasMap(oraQBlock.catalystProjectList)
    exps.map(e => replaceAlias(e, aliasMap))
  }

  private def pushSemiJoin : Option[OraQueryBlock] = {
    val joinOp = pushdownCatalystOp

    for (
      _leftOraExprs <- OraExpressions.unapplySeq(leftKeys);
      _rightOraExprs <- OraExpressions.unapplySeq(dereference(rightKeys, rightQBlk));
      outProjections <- OraExpressions.unapplySeq(joinOp.output)
    ) yield {
      val leftOraExprs = _leftOraExprs.map(OraExpression.fixForJoinCond)
      val rightOraExprs = _rightOraExprs.map(OraExpression.fixForJoinCond)
      val oraExpression: OraExpression = OraSubQueryJoin(
        joinOp,
        leftOraExprs,
        SQLSnippet.IN,
        rightQBlk.copyBlock(select = rightOraExprs))
      val newFil = currQBlk.where.map(f =>
        OraBinaryOpExpression(AND,
          And(f.catalystExpr, oraExpression.catalystExpr),
          f, oraExpression
        )
      ).getOrElse(oraExpression)

      currQBlk.copyBlock(
        select = outProjections,
        where = Some(newFil),
        catalystOp = Some(joinOp),
        catalystProjectList = joinOp.output
      )
    }
  }

  /**
   * Handle `not in` and `not exists` translation.
   *  - `not exists` case is when there is equiJoin conditions in the NotInJoinPattern
   *    (notInLKeys, notInRkeys are empty)
   *  - `not in` has 2 cases
   *    - when there is are correlated conditions, i.e. `leftKeys.nonEmpty`
   *    - when there is no correlated conditions, i.e. `leftKeys.isEmpty`
   *
   * The correlated condition is constructed by [[EqualTo]] check on each
   * `outerCorrOEs` and `innerCorrOEs`. `outerCorrOEs` are wrapped in
   * [[org.apache.spark.sql.oracle.expressions.Named.OraOuterRef]].
   *
   * [[OraFixColumnNames]] then tags the outer [[org.apache.spark.sql.oracle.expressions.Named.OraColumnRef]]
   * with the correct oracle-sql name from the outer Query Block.
   *
   * @return
   */
  private def pushNot: Option[OraQueryBlock] = {
    val joinOp = pushdownCatalystOp
    val notInConjuncts = joinCond.map(splitConjunctivePredicates).getOrElse(Seq.empty)

    val notInJoinPattern = NotInJoinPattern(joinOp)
    val notInJoinKeys: Option[Seq[(Expression, Expression)]] =
      OraSparkUtils.sequence(notInConjuncts.map(notInJoinPattern.unapply(_)))
    val (notInLKeys, notInRkeys) = notInJoinKeys.map(_.unzip).getOrElse((Seq.empty, Seq.empty))
    val notExists = notInLKeys.isEmpty
    val corrPredicate = leftKeys.nonEmpty

    for (
      _outerCorrOEs <- OraExpressions.unapplySeq(leftKeys);
      _outerNotInOEs <- OraExpressions.unapplySeq(notInLKeys);
      _innerCorrOEs <- OraExpressions.unapplySeq(dereference(rightKeys, rightQBlk));
      _innerNotInOEs <- OraExpressions.unapplySeq(dereference(notInRkeys, rightQBlk));
      outProjections <- OraExpressions.unapplySeq(joinOp.output)
    ) yield {


      val outerCorrOEs = _outerCorrOEs.
        map(OraExpression.fixForJoinCond).
        map(Named.makeReferencesOuter)

      val innerCorrOEs = _innerCorrOEs.map(OraExpression.fixForJoinCond)
      val outerNotInOEs = _outerNotInOEs.map(OraExpression.fixForJoinCond)
      val innerNotInOEs = _innerNotInOEs.map(OraExpression.fixForJoinCond)


      /**
       * The correlated condition is constructed by [[EqualTo]] check on each
       * `outerCorrOEs` and `innerCorrOEs`. `outerCorrOEs` are wrapped in
       * [[org.apache.spark.sql.oracle.expressions.Named.OraOuterRef]]
       */
      val innerCorrCond : OraExpression = if (corrPredicate) {
        val conds: Seq[OraExpression] = outerCorrOEs.zip(innerCorrOEs).map {
          case (o, i) =>
            val cE = EqualTo(o.catalystExpr, i.catalystExpr)
            OraBinaryOpExpression(cE.symbol, cE, o, i)
        }

        conds.tail.fold(conds.head) { (l, r) =>
          OraBinaryOpExpression(AND, And(l.catalystExpr, r.catalystExpr), l, r)
        }
      } else null

      /**
       * Update the `rightQBlk`:
       * - add the `innerCorrCond` to the where clause
       * - change the selectList to the `innerNotInOEs` list
       */
      val newInnerQBlk = {
        val rBlck = if (rightQBlk.canApplyFilter) {
          rightQBlk
        } else {
          OraQueryBlock.newBlockOnCurrent(rightQBlk)
        }

        val innerWhere : Option[OraExpression] = if (corrPredicate) {
          val oE = rBlck.where.map(f =>
            OraBinaryOpExpression(AND,
              And(f.catalystExpr, innerCorrCond.catalystExpr),
              f, innerCorrCond
            )
          ).getOrElse(innerCorrCond)
          Some(oE)
        } else rBlck.where

        val innerSelect = if (notExists) {
          Seq(new OraLiteralSql("1"))
        } else {
          innerNotInOEs
        }

        rBlck.copyBlock(
          select = innerSelect,
          where = innerWhere
        )
      }

      val joinSQOp = if (notExists) {
        SQLSnippet.NOT_EXISTS
      } else {
        SQLSnippet.NOT_IN
      }

      val subQryOE: OraExpression = OraSubQueryJoin(
        joinOp,
        outerNotInOEs,
        joinSQOp,
        newInnerQBlk)

      val newFil = currQBlk.where.map(f =>
        OraBinaryOpExpression(AND,
          And(f.catalystExpr, subQryOE.catalystExpr),
          f, subQryOE
        )
      ).getOrElse(subQryOE)

      currQBlk.copyBlock(
        select = outProjections,
        where = Some(newFil),
        catalystOp = Some(joinOp),
        catalystProjectList = joinOp.output
      )
    }

  }

  private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    if (currQBlk.canApply(pushdownCatalystOp)) {

      joinType match {
        case LeftSemi if leftKeys.nonEmpty && !joinCond.isDefined => pushSemiJoin
        case LeftAnti => pushNot
        case _ => None
      }
    } else None
  }
}
