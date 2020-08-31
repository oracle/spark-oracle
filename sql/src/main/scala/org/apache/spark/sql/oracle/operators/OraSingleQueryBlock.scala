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

import org.apache.spark.sql.catalyst.expressions.{NamedExpression, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Filter, GlobalLimit, Join, LogicalPlan, Project, Sort, Window}
import org.apache.spark.sql.oracle.{OraSQLImplicits, SQLSnippet}
import org.apache.spark.sql.oracle.expressions.{OraExpression, OraLiteralSql}
import org.apache.spark.sql.oracle.expressions.Named.OraColumnRef
import org.apache.spark.sql.oracle.expressions.Predicates.RownumLimit
import org.apache.spark.sql.oracle.querysplit.OraSplitStrategy

trait OraQueryBlockState { self: OraSingleQueryBlock =>

  lazy val hasComputedShape: Boolean =
    select.exists(o => !o.isInstanceOf[OraColumnRef])
  lazy val hasOuterJoin: Boolean =
    joins.exists(j => Set[JoinType](LeftOuter, RightOuter, FullOuter).contains(j.joinType))
  lazy val hasFilter: Boolean = where.isDefined
  private lazy val hasAggregations : Boolean = {
    select.map {oE =>
      oE.map(_.catalystExpr).collect {
        case aE : AggregateExpression => aE
      }
    }.flatten.nonEmpty
  }
  lazy val hasAggregate : Boolean = {
    groupBy.isDefined || hasAggregations
  }

  lazy val hasOrder : Boolean = {
    orderBy.isDefined
  }

  /**
   * Query Block collapsing rules for window expressions:
   *  -  don't collapse multi Window operators because parent
   *     Window expression may be calculated based on a child
   *     Window expression.
   *  - don't 'rewrite' Project and Filter expressions so
   *    they are collapsed into the current query block
   *    because multi invocations of a window expression
   *    may not be combined and calculated once in Oracle.
   *  - don't collapse a Sort into a Query Block with
   *    window expressions because Sort expression may
   *    refer to window expression. (TODO, this maybe ok)
   */
  lazy val hasWindow : Boolean = {
    select.map {oE =>
      oE.map(_.catalystExpr).collect {
        case wE : WindowExpression => wE
      }
    }.flatten.nonEmpty
  }

  lazy val hasJoins = joins.nonEmpty
  lazy val hasLatJoin = latJoin.isDefined

  def hasRowLimit : Boolean =
    where.map {w =>
      w.find(oE => oE.isInstanceOf[RownumLimit]).isDefined
    }.getOrElse(false)

  def canApply(plan: LogicalPlan): Boolean = plan match {
    case p: Project => !(hasOrder || hasWindow)
    case s: Sort => !(hasOrder || hasWindow)
    case w: Window => !(hasOrder || hasWindow || hasComputedShape)
    case f: Filter => !(hasOrder || hasOuterJoin || hasAggregate || hasWindow)
    case j@Join(_, _, (LeftOuter | RightOuter | FullOuter), _, _) =>
      !(hasComputedShape || hasFilter || hasAggregate || hasLatJoin || hasOrder)
    case j: Join => !(hasComputedShape || hasAggregate || hasLatJoin || hasOrder)
    case e: Expand => !(hasComputedShape || hasAggregate || hasLatJoin || hasOrder)
    case a: Aggregate => !(hasComputedShape || hasAggregate || hasOrder)
    case gl : GlobalLimit => !(hasOuterJoin || hasAggregate || hasOrder || hasWindow)
  }

  def canApplyFilter : Boolean = !(hasOrder || hasOuterJoin || hasAggregate)

}

trait OraQueryBlockSQLSnippets {self: OraSingleQueryBlock =>

  import OraSQLImplicits._

  private def sourceSnippet(implicit srcSnipOverride :
  OraTableScan => Option[SQLSnippet] = t => None) : SQLSnippet = {
    val srcSQL = source match {
      case ot : OraTableScan => srcSnipOverride(ot).getOrElse(SQLSnippet.tableQualId(ot.oraTable))
      case oQ : OraQueryBlock => SQLSnippet.subQuery(oQ.orasql)
    }
    val qualifier : SQLSnippet =
      getSourceAlias.map(sA => SQLSnippet.colRef(sA)).getOrElse(SQLSnippet.empty)
    srcSQL + qualifier
  }

  protected def selectListSQL : Seq[SQLSnippet] =
    if (select.nonEmpty) {
      select.map(_.reifyLiterals.orasql)
    } else {
      /*
       * Spark Optimized Plan may contain Aggregate operators with no projections.
       * For example tpcds.q38 generates and optimized plan like this:
       * GlobalLimit 100
        +- LocalLimit 100
           +- Aggregate [count(1) AS count(1)#230L]
              +- Aggregate [c_last_name#60, c_first_name#59, d_date#25]
                 +- Aggregate [c_last_name#60, c_first_name#59, d_date#25], [c_last_name#60, c_first_name#59, d_date#25]
                    +- Aggregate [c_last_name#60, c_first_name#59, d_date#25], [c_last_name#60, c_first_name#59, d_date#25]
       * - the first aggregate is for intersect
       * - the second is for the distinct
       * - the 3rd is for the subquery block formed by the outermost `select  count(*) from (...`
       * - the 4th is for the `count(*)`
       *
       * So the generated OraPlan looks like this:
       * OraSingleQueryBlock [count(1)#230L], [oracolumnref(count(1)#230L)], rownumlimit(100)
        +- OraSingleQueryBlock [count(1) AS count(1)#230L], [oraalias(count(1) AS count(1)#230L)], List()
           +- OraSingleQueryBlock List(oracolumnref(c_last_name#60), oracolumnref(c_first_name#59), oracolumnref(d_date#25))
              +- OraSingleQueryBlock [c_last_name#60, c_first_name#59, d_date#25], [oracolumnref(c_last_name#60), oracolumnref(c_first_name#59), oracolumnref(d_date#25)], List(oracolumnref(c_last_name#60), oracolumnref(c_first_name#59), oracolumnref(d_date#25))
                 +- OraSingleQueryBlock [c_last_name#60, c_first_name#59, d_date#25], [oracolumnref(c_last_name#60), oracolumnref(c_first_name#59), oracolumnref(d_date#25)], orabinaryopexpression((((isnotnull(SS_CUSTOMER_SK#3) AND ((isnotnull(D_MONTH_SEQ#26) AND (D_MONTH_SEQ#26 >= 1200.000000000000000000)) AND (D_MONTH_SEQ#26 <= 1211.000000000000000000))) AND (((c_last_name#60 <=> c_last_name#140) AND (c_first_name#59 <=> c_first_name#139)) AND (d_date#25 <=> d_date#105))) AND (((c_last_name#60 <=> c_last_name#220) AND (c_first_name#59 <=> c_first_name#219)) AND (d_date#25 <=> d_date#185)))), List(oracolumnref(c_last_name#60), oracolumnref(c_first_name#59), oracolumnref(d_date#25))
                    :- OraTableScan TPCDS.STORE_SALES, [SS_CUSTOMER_SK#3, SS_SOLD_DATE_SK#0]
                    :- OraTableScan TPCDS.DATE_DIM, [D_DATE_SK#23, D_DATE#25, D_MONTH_SEQ#26]
                    +- OraTableScan TPCDS.CUSTOMER, [C_CUSTOMER_SK#51, C_FIRST_NAME#59, C_LAST_NAME#60]
       * where the second OraSingleQueryBlock has no projections.
       */
      Seq(new OraLiteralSql("1").orasql)
    }

  def sourcesSQL(implicit srcSnipOverride : OraTableScan => Option[SQLSnippet] = t => None)
  : SQLSnippet = {
    var ss = sourceSnippet ++ joins.map(_.orasql)
    if (latJoin.isDefined) {
      ss = ss + osql" , lateral ( ${latJoin.get.orasql} )"
    }
    ss
  }

  protected def whereConditionSQL = where.map(_.orasql)

  protected def groupByListSQL = groupBy.map(_.map(_.reifyLiterals.orasql))

  protected def orderByListSQL : Option[Seq[SQLSnippet]] =
    orderBy.map(_.map(_.reifyLiterals.orasql))
}

/**
 * Represents a Oracle SQL query block.
 *
 * @param source  the initial [[OraPlan]] on which this QueryBlock is layered.
 * @param joins   the `inner` or `outer` joins in this query block.
 * @param select  the projected expressions of this query block.
 * @param where   an optional filter expression
 * @param groupBy optional aggregation expressions.
 * @param orderBy optional sort order expressions.
 */
case class OraSingleQueryBlock(source: OraPlan,
                               joins: Seq[OraJoinClause],
                               latJoin : Option[OraLateralJoin],
                               select: Seq[OraExpression],
                               where: Option[OraExpression],
                               groupBy: Option[Seq[OraExpression]],
                               catalystOp: Option[LogicalPlan],
                               catalystProjectList: Seq[NamedExpression],
                               orderBy: Option[Seq[OraExpression]])
  extends OraQueryBlock with OraQueryBlockState with OraQueryBlockSQLSnippets {

  val children: Seq[OraPlan] = Seq(source) ++ joins.map(_.joinSrc)

  override def stringArgs: Iterator[Any] =
    Iterator(catalystProjectList, select, where, groupBy, orderBy)

  override def orasql: SQLSnippet = {
    SQLSnippet.select(selectListSQL : _*).
      from(sourcesSQL).
      where(whereConditionSQL).
      groupBy(groupByListSQL).
      orderBy(orderByListSQL)
  }

  override def splitOraSQL(dbSplitId : Int, splitStrategy : OraSplitStrategy)
  : SQLSnippet = {
    val sqlSnip = SQLSnippet.select(selectListSQL : _*).
      from(sourcesSQL(ot => splitStrategy.splitOraSQL(ot, dbSplitId))).
      where(whereConditionSQL).
      groupBy(groupByListSQL).
      orderBy(orderByListSQL)
    splitStrategy.associateFetchClause(sqlSnip, !orderBy.isDefined, select.size, dbSplitId)
  }

  override def copyBlock(source: OraPlan = source,
                         joins: Seq[OraJoinClause] = joins,
                         latJoin : Option[OraLateralJoin] = latJoin,
                         select: Seq[OraExpression] = select,
                         where: Option[OraExpression] = where,
                         groupBy: Option[Seq[OraExpression]] = groupBy,
                         catalystOp: Option[LogicalPlan] = catalystOp,
                         catalystProjectList: Seq[NamedExpression] = catalystProjectList,
                         orderBy: Option[Seq[OraExpression]] = orderBy) : OraQueryBlock =
    this.copy(
      source = source,
      joins = joins,
      latJoin = latJoin,
      select = select,
      where = where,
      groupBy = groupBy,
      catalystOp = catalystOp,
      catalystProjectList = catalystProjectList,
      orderBy = orderBy
    )
}
