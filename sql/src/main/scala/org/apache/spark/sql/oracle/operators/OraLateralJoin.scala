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
package org.apache.spark.sql.oracle.operators

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.oracle.{OraSparkUtils, SQLSnippet}
import org.apache.spark.sql.oracle.expressions.{Named, OraExpression, OraExpressions}
import org.apache.spark.sql.types.{DataType, NullType}

case class OraLatJoinProjEntry(oraExpr: OraExpression, outAttr: Option[Attribute])
    extends OraExpression {

  def getOraFixedAlias : Option[String] = getTagValue(Named.ORA_ALIAS_TAG)
  def setOraFixedAlias(alias : String) : Unit = {
    setTagValue(Named.ORA_ALIAS_TAG, alias)
  }
  def clearOraFixedAlias : Unit = unsetTagValue(Named.ORA_ALIAS_TAG)

  private def outNmInOraSQL : Option[String] = Option(
    getOraFixedAlias.getOrElse(outAttr.map(_.name).getOrElse(null))
  )

  override def orasql: SQLSnippet = {
    val oAlias = outNmInOraSQL
    if (!oAlias.isDefined) {
      oraExpr.orasql
    } else {
      osql"${oraExpr} ${SQLSnippet.colRef(oAlias.get)}"
    }
  }

  override def catalystExpr: Expression = oraExpr.catalystExpr

  override def children: Seq[OraExpression] = Seq(oraExpr)
}

case class OraLatJoinProjectionExpr(children: Seq[Expression]) extends Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = NullType
}

case class OraLatJoinProjection(projectList: Seq[OraLatJoinProjEntry]) extends OraExpression {

  override def orasql: SQLSnippet =
    osql"select ${SQLSnippet.csv(projectList.map(_.reifyLiterals.orasql): _*)} from dual"

  lazy val catalystExpr: Expression = OraLatJoinProjectionExpr(projectList.map(_.catalystExpr))

  override def children: Seq[OraExpression] = projectList
}

case class OraLatJoinExpr(children: Seq[Expression]) extends Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = NullType
}

case class OraLateralJoin(expand: Expand, projections: Seq[OraLatJoinProjection])
    extends OraExpression {

  val uAll = osql"union all"
  override def orasql: SQLSnippet =
    SQLSnippet.join(projections.map(_.orasql), uAll, true)

  override def children: Seq[OraExpression] = projections

  override def catalystExpr: Expression = OraLatJoinExpr(expand.output)

}

/**
 * Spark evaluates `count distinct`, `sum distinct` and `cube/rollup` calculations by
 * ''expanding''(and ''tagging'') input tows into multiple rows. Each ''class of rows''
 * is used for a different aggregation.
 *
 * '''Count/Sum Distinct:'''
 *
 * For example for the following distinct calculations:
 * {{{
 *   select c_int as ci, c_long as cl,
 *        sum(distinct c_decimal_scale_8) + count(distinct c_decimal_scale_5)
 * from sparktest.unit_test
 * group by  c_int + c_long, c_int, c_long
 * }}}
 *
 * The optimized Spark Plan is:
 * {{{
 * Aggregate [(CAST(oracle.sparktest.unit_test.`c_int` AS BIGINT) + oracle.sparktest.unit_test.`c_long`)#34L, c_int#12, c_long#13L], [c_int#12 AS ci#0, c_long#13L AS cl#1L, CheckOverflow((promote_precision(cast(sum(if ((gid#33 = 2)) oracle.sparktest.unit_test.`c_decimal_scale_8`#36 else null) as decimal(36,8))) + promote_precision(cast(cast(count(if ((gid#33 = 1)) oracle.sparktest.unit_test.`c_decimal_scale_5`#35 else null) as decimal(20,0)) as decimal(36,8)))), DecimalType(36,8), true) AS (CAST(sum(DISTINCT c_decimal_scale_8) AS DECIMAL(36,8)) + CAST(CAST(count(DISTINCT c_decimal_scale_5) AS DECIMAL(20,0)) AS DECIMAL(36,8)))#21]
 * +- Aggregate [(CAST(oracle.sparktest.unit_test.`c_int` AS BIGINT) + oracle.sparktest.unit_test.`c_long`)#34L, c_int#12, c_long#13L, oracle.sparktest.unit_test.`c_decimal_scale_5`#35, oracle.sparktest.unit_test.`c_decimal_scale_8`#36, gid#33], [(CAST(oracle.sparktest.unit_test.`c_int` AS BIGINT) + oracle.sparktest.unit_test.`c_long`)#34L, c_int#12, c_long#13L, oracle.sparktest.unit_test.`c_decimal_scale_5`#35, oracle.sparktest.unit_test.`c_decimal_scale_8`#36, gid#33]
 *    +- Expand [ArrayBuffer((cast(c_int#12 as bigint) + c_long#13L), c_int#12, c_long#13L, c_decimal_scale_5#15, null, 1), ArrayBuffer((cast(c_int#12 as bigint) + c_long#13L), c_int#12, c_long#13L, null, c_decimal_scale_8#16, 2)], [(CAST(oracle.sparktest.unit_test.`c_int` AS BIGINT) + oracle.sparktest.unit_test.`c_long`)#34L, c_int#12, c_long#13L, oracle.sparktest.unit_test.`c_decimal_scale_5`#35, oracle.sparktest.unit_test.`c_decimal_scale_8`#36, gid#33]
 *       +- RelationV2[C_INT#12, C_LONG#13L, C_DECIMAL_SCALE_5#15, C_DECIMAL_SCALE_8#16] SPARKTEST.UNIT_TEST
 * }}}
 *
 *  - each input row is expanded to 2 rows
 *    - 1 row is tagged with `gid = 1`; other is tagged with `gid = 2`
 *    - the `gid=1` row has `c_decimal_scale_8 = null`
 *    - the `gid=2` row has `c_decimal_scale_5 = null`
 *    - the rows also calculate the grouping expressions; in this example they are `c_int + c_long, c_int, c_long`
 *    - the rows also output any columns needed in aggregations; in this case they are `c_decimal_scale_8, c_decimal_scale_5`
 *  - The first Aggregate operation is for making the values distinct
 *    - it groups the incoming rows by `c_int + c_long, c_int, c_long, c_decimal_scale_5, c_decimal_scale_8, gid`
 * - The second Aggregate operation calculates the aggregates:
 * {{{
 *   Group By: c_int + c_long, c_int, c_long
 *   Aggregates:
 *     // gid=2 rows used to calculate sum distinct
 *     sum(if (gid =2) c_decimal_scale_8 else null)
 *    // gid=1 rows used to calculate count distinct
 *    count(if (gid =1) c_decimal_scale_5 else null
 * }}}
 *
 * When translating to Oracle SQL, we use oracle's
 * [[https://oracle-base.com/articles/12c/lateral-inline-views-cross-apply-and-outer-apply-joins-12cr1 lateral inline view]].
 * So for the above example the oracle sql generated is(showing SQL w/o Aggregate pushdown):
 * {{{
 *   select "(CAST(oracle.sparktest.unit_test.`c_int` AS BIGINT) + oracle.sparktest.unit_test.`c_long`)",
 *        "C_INT", "C_LONG",
 *        "oracle.sparktest.unit_test.`c_decimal_scale_5`",
 *        "oracle.sparktest.unit_test.`c_decimal_scale_8`",
 *        "gid"
 * from SPARKTEST.UNIT_TEST   ,
 *      lateral (
 *         select ("C_INT" + "C_LONG") "(CAST(oracle.sparktest.unit_test.`c_int` AS BIGINT) + oracle.sparktest.unit_test.`c_long`)",
 *                "C_DECIMAL_SCALE_5" "oracle.sparktest.unit_test.`c_decimal_scale_5`",
 *                null "oracle.sparktest.unit_test.`c_decimal_scale_8`",
 *                1 "gid"
 *         from dual
 *         union all
 *         select ("C_INT" + "C_LONG"), null, "C_DECIMAL_SCALE_8", 2 from dual
 *      )
 * }}}
 *
 * '''Cube/Rollup''':
 *
 * For example for the following rollup calculations:
 * {{{
 *   select i_category
 *                   ,d_year
 *                   ,d_qoy
 *                   ,d_moy
 *                   ,s_store_id
 *                   ,sum(ss_sales_price*ss_quantity) sumsales
 *             from store_sales
 *                 ,date_dim
 *                 ,store
 *                 ,item
 *        where  ss_sold_date_sk=d_date_sk
 *           and ss_item_sk=i_item_sk
 *           and ss_store_sk = s_store_sk
 *           and d_month_seq between 1200 and 1200+11
 *        group by  rollup(i_category, d_year, d_qoy, d_moy,s_store_id)
 * }}}
 *
 * The optimized Spark Plan is:
 * {{{
 * Aggregate [i_category#110, d_year#111, d_qoy#112, d_moy#113, s_store_id#114, spark_grouping_id#109L], [i_category#110, d_year#111, d_qoy#112, d_moy#113, s_store_id#114, sum(CheckOverflow((promote_precision(ss_sales_price#14) * promote_precision(ss_quantity#11)), DecimalType(38,6), true)) AS sumsales#0]
 * +- Expand [List(SS_QUANTITY#11, SS_SALES_PRICE#14, i_category#93, d_year#30, d_qoy#34, d_moy#32, s_store_id#53, 0), List(SS_QUANTITY#11, SS_SALES_PRICE#14, i_category#93, d_year#30, d_qoy#34, d_moy#32, null, 1), List(SS_QUANTITY#11, SS_SALES_PRICE#14, i_category#93, d_year#30, d_qoy#34, null, null, 3), List(SS_QUANTITY#11, SS_SALES_PRICE#14, i_category#93, d_year#30, null, null, null, 7), List(SS_QUANTITY#11, SS_SALES_PRICE#14, i_category#93, null, null, null, null, 15), List(SS_QUANTITY#11, SS_SALES_PRICE#14, null, null, null, null, null, 31)], [SS_QUANTITY#11, SS_SALES_PRICE#14, i_category#110, d_year#111, d_qoy#112, d_moy#113, s_store_id#114, spark_grouping_id#109L]
 *    +- Project [SS_QUANTITY#11, SS_SALES_PRICE#14, i_category#93, d_year#30, d_qoy#34, d_moy#32, s_store_id#53]
 *       +- Join Inner, (ss_item_sk#3 = i_item_sk#81)
 *          :- Project [SS_ITEM_SK#3, SS_QUANTITY#11, SS_SALES_PRICE#14, D_YEAR#30, D_MOY#32, D_QOY#34, S_STORE_ID#53]
 *          :  +- Join Inner, (ss_store_sk#8 = s_store_sk#52)
 *          :     :- Project [SS_ITEM_SK#3, SS_STORE_SK#8, SS_QUANTITY#11, SS_SALES_PRICE#14, D_YEAR#30, D_MOY#32, D_QOY#34]
 *          :     :  +- Join Inner, (ss_sold_date_sk#1 = d_date_sk#24)
 *          :     :     :- Project [SS_SOLD_DATE_SK#1, SS_ITEM_SK#3, SS_STORE_SK#8, SS_QUANTITY#11, SS_SALES_PRICE#14]
 *          :     :     :  +- Filter isnotnull(SS_STORE_SK#8)
 *          :     :     :     +- RelationV2[SS_ITEM_SK#3, SS_STORE_SK#8, SS_QUANTITY#11, SS_SALES_PRICE#14, SS_SOLD_DATE_SK#1] TPCDS.STORE_SALES
 *          :     :     +- Project [D_DATE_SK#24, D_YEAR#30, D_MOY#32, D_QOY#34]
 *          :     :        +- Filter ((isnotnull(D_MONTH_SEQ#27) AND (D_MONTH_SEQ#27 >= 1200.000000000000000000)) AND (D_MONTH_SEQ#27 <= 1211.000000000000000000))
 *          :     :           +- RelationV2[D_DATE_SK#24, D_MONTH_SEQ#27, D_YEAR#30, D_MOY#32, D_QOY#34] TPCDS.DATE_DIM
 *          :     +- RelationV2[S_STORE_SK#52, S_STORE_ID#53] TPCDS.STORE
 *          +- RelationV2[I_ITEM_SK#81, I_CATEGORY#93] TPCDS.ITEM
 * }}}
 *  - each input(after all joins are done) row is expanded for each rollup grouping set; in this case
 *    there are 5 groups, so each row is expanded 5 times.
 *  - each expanded row is assigned a `spark_grouping_id` value. This is a integer value whose
 *    bit string identifies the grouping set that this row belongs to.
 *  - So `spark_grouping_id=1` implies this is the grouping_set: `(i_cat, year,qoy, moy)`
 *    - not `0` implies column inclusion in grouping set
 *  - `spark_grouping_id=7` implies this is the grouping_set: `(i_cat, year)`
 *  - The aggregation is done on the columns plus the `spark_grouping_id` column.
 *    - So aggregation operation :
 *      - Group by: `i_category, d_year, d_qoy, d_moy,s_store_id, spark_grouping_id`
 *      - Aggregations: `sum(ss_sales_price*ss_quantity)`
 *  - The output includes the `spark_grouping_id` column that can be used to identify the
 *    grouping set of the output row.
 *
 * When translating to Oracle SQL, we use oracle's
 * [[https://oracle-base.com/articles/12c/lateral-inline-views-cross-apply-and-outer-apply-joins-12cr1 lateral inline view]].
 * So for the above example the oracle sql generated is(showing SQL w/o Aggregate pushdown):
 *
 * {{{
 *   SELECT "SS_QUANTITY",
 *        "SS_SALES_PRICE",
 *        "i_category",
 *        "d_year",
 *        "d_qoy",
 *        "d_moy",
 *        "s_store_id",
 *        "spark_grouping_id"
 * FROM TPCDS.STORE_SALES
 * JOIN TPCDS.DATE_DIM ON ("SS_SOLD_DATE_SK" = "D_DATE_SK")
 * JOIN TPCDS.STORE ON ("SS_STORE_SK" = "S_STORE_SK")
 * JOIN TPCDS.ITEM ON ("SS_ITEM_SK" = "I_ITEM_SK") , LATERAL
 *   (SELECT "I_CATEGORY" "i_category",
 *           "D_YEAR" "d_year",
 *           "D_QOY" "d_qoy",
 *           "D_MOY" "d_moy",
 *           "S_STORE_ID" "s_store_id",
 *           0 "spark_grouping_id"
 *    FROM dual
 *    UNION ALL SELECT "I_CATEGORY",
 *                     "D_YEAR",
 *                     "D_QOY",
 *                     "D_MOY",
 *                     NULL,
 *                     1
 *    FROM dual
 *    UNION ALL SELECT "I_CATEGORY",
 *                     "D_YEAR",
 *                     "D_QOY",
 *                     NULL,
 *                     NULL,
 *                     3
 *    FROM dual
 *    UNION ALL SELECT "I_CATEGORY",
 *                     "D_YEAR",
 *                     NULL,
 *                     NULL,
 *                     NULL,
 *                     7
 *    FROM dual
 *    UNION ALL SELECT "I_CATEGORY",
 *                     NULL,
 *                     NULL,
 *                     NULL,
 *                     NULL,
 *                     15
 *    FROM dual
 *    UNION ALL SELECT NULL,
 *                     NULL,
 *                     NULL,
 *                     NULL,
 *                     NULL,
 *                     31
 *    FROM dual)
 * WHERE (("SS_STORE_SK" IS NOT NULL
 *         AND "SS_SOLD_DATE_SK" IS NOT NULL)
 *        AND (("D_MONTH_SEQ" IS NOT NULL
 *              AND ("D_MONTH_SEQ" >= 1200.000000000000000000))
 *             AND ("D_MONTH_SEQ" <= 1211.000000000000000000)))
 * }}}
 */
object OraLateralJoin {
  def unapply(expand: Expand): Option[(OraLateralJoin, Seq[OraExpression])] = {

    val inExprIdSet = expand.child.output.map(a => a.exprId).toSet

    def latExprAlias(attr: Attribute): Option[Attribute] =
      if (!inExprIdSet.contains(attr.exprId)) {
        Some(attr)
      } else None

    /**
     * Oracle Lateral Inline View semantics imply that its output includes all
     * the columns of its input. If we include the input columns just as the
     * Spark Expand operator does than we get a ''duplicate names'' SQL error.
     * So here we are computing what positions Expand projections need to
     * be in the lateral view.
     *
     * The logic to choose these positions is based on the observation that
     * expand projections that merely represent input columns have the same
     * [[ExprId]] in the Expand output.
     */
    val latProjectionsToInclude: Set[Int] =
      (for ((attr, i) <- expand.output.zipWithIndex
            if (!inExprIdSet.contains(attr.exprId))) yield {
        i
      }).toSet

    /**
     * filter out projections that don't need to be in select list
     * of lateral inline view.
     *
     * @param projs
     * @return
     */
    def filterProjectionsToSkip(projs: Seq[Expression]) = {
      projs.zipWithIndex
        .filter {
          case (_, i) => latProjectionsToInclude.contains(i)
        }
        .map(_._1)
    }

    for (expOraProjLists <- OraSparkUtils.sequence(
           expand.projections.map(pl => OraExpressions.unapplySeq(filterProjectionsToSkip(pl))));
         oraOut <- OraExpressions.unapplySeq(expand.output)) yield {
      val firstProjList = {
        val oraProjs = expOraProjLists.head
        oraProjs.zip(filterProjectionsToSkip(expand.output)).map {
          case (oE, attr) => OraLatJoinProjEntry(oE, latExprAlias(attr.asInstanceOf[Attribute]))
        }
      }
      val restProjLists = expOraProjLists.tail.map { oraProjs =>
        oraProjs.map(oE => OraLatJoinProjEntry(oE, None))
      }
      val oraJoinProjs = (firstProjList +: restProjLists).map(OraLatJoinProjection)
      (OraLateralJoin(expand, oraJoinProjs), oraOut)
    }
  }
}
