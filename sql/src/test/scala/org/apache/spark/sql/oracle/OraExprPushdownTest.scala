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

package org.apache.spark.sql.oracle

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.connector.catalog.oracle.OraMetadataMgrInternalTest
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.expressions.{Casts, OraExpression}

class OraExprPushdownTest extends AbstractTest with OraMetadataMgrInternalTest {

  lazy val parser = TestOracleHive.sparkSession.sessionState.sqlParser
  lazy val analyzer = TestOracleHive.sparkSession.sessionState.analyzer

  lazy val unit_test_table = TestOracleHive.sparkSession.table("unit_test")

  lazy val typeCoercionRules = TypeCoercion.typeCoercionRules

  protected def resolveExpr(exprStr: String,
                            plan: LogicalPlan = unit_test_table.queryExecution.analyzed
                           ): Expression = {
    val expr = parser.parseExpression(exprStr)
    val r1 = analyzer.resolveExpressionBottomUp(expr, plan, false)

    val fPlan = Filter(r1, plan)
    val aPlan = analyzer.executeAndCheck(fPlan, new QueryPlanningTracker)
    aPlan.asInstanceOf[Filter].condition
  }

  protected def resolveExpr(exprStr: String,
                            tNm: String): Expression = {
    resolveExpr(exprStr,
      TestOracleHive.sparkSession.table(tNm).queryExecution.analyzed
    )
  }

  protected def oraExpression(expr: Expression) =
    OraExpression.unapply(expr)

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestOracleHive.sql("use oracle.sparktest")
  }

  def test(nm: String,
           exprStr: String,
           plan: () => LogicalPlan): Unit = test(nm) { td =>
    val expr = resolveExpr(exprStr, plan())
    expr match {
      // scalastyle:off println
      case cE@OraExpression(oraExpr) =>
        println(
          s"""OraExpression for spark expression: ${cE.sql}:
             |  ${oraExpr.reifyLiterals.orasql.sql}""".stripMargin
        )
      // scalastyle:on println
      case _ =>
        throw new AnalysisException(
          s"""for
             |  ${exprStr}
             |no oracle expression
             |spark expression: ${expr.toString()}""".stripMargin
        )
    }
  }

  val castingExpressions : Seq[String] = {
    val from_str = Seq(
      """cast('1' as long) = 1""",
      """cast('1.0' as double) = 1.0""",
      """cast('1' as long) = 1""",
      // oracle default date format is 'DD-MON-RR'
      """cast('01-JAN-20' as date) = cast('01-JAN-20 12.00.00.000000000 AM' as timestamp)""",
      """cast('t' as boolean)""",

    )

    val to_str = Seq(
      """cast(1 as string) = '1'""",
      """cast(1.0 as string) = '1.0'""",
      """cast(1L as string) = '1'""",
      """cast(cast('01-JAN-20' as date) as string) = '01-JAN-20'""",
      """cast(c_date as string) = '01-JAN-20'""",
      """cast(cast('01-JAN-20' as timestamp) as string) = '01-JAN-20 12.00.00.000000000 AM'""",
      """cast(c_timestamp as string) = '01-JAN-20 12.00.00.000000000 AM'"""
    )

    val from_date = Seq(
      """cast(c_date as long) = 0""",
      """cast(c_date as double) = 0.0""",
      """cast(c_date as string) = '01-JAN-20'""",
      """c_date = c_timestamp""",
      """cast(c_date as boolean) is null"""
    )
    val to_date = Seq(
      // """cast(0 as date) = c_date""", cannot cast int to date
      """cast('01-JAN-20' as date) = c_date""",
      """cast(c_timestamp as date) = c_date"""
    )

    val from_ts = Seq(
      """cast(0 as timestamp) = c_timestamp""",
      """cast(c_timestamp as long) = 0""",
      """cast(c_timestamp as double) = 0.0""",
      """cast(c_timestamp as string) = '01-JAN-20 12.00.00.000000000 AM'"""
    )
    val to_ts = Seq(
      """cast(1 as timestamp) = c_timestamp""",
      """cast(1.0 as timestamp) = c_timestamp""",
      """cast(c_varchar2_10 as timestamp) = c_timestamp""",
      """cast(c_date as timestamp) = c_timestamp""",
    )
    from_str ++ to_str ++ from_date ++ to_date ++ from_ts ++ to_ts
  }

  val pushableExpressions = Seq(
    """
      |c_int > 1 and
      |    c_date is null and
      |    c_timestamp is not null
      |""".stripMargin,
    "(c_int % 5) < (c_int * 5)",
    "abs(c_long) > c_long",
    """
      |case
      |  when c_int > 0 then "positive"
      |  when c_int < 0 then "negative"
      |  else "zero"
      |end in ("positive", "zero")""".stripMargin,
    """
      |c_int > 1 and
      |    (c_int % 5) < (c_int * 5) and
      |    abs(c_long) > c_long and
      |    c_date is null and
      |    c_timestamp is not null
      |""".stripMargin,
    """
      |c_int > 1 and
      |    case
      |       when c_int > 0 then "positive"
      |       when c_int < 0 then "negative"
      |       else "zero"
      |     end in ("positive", "zero") and
      |    (c_int % 5) < (c_int * 5) and
      |    abs(c_long) > c_long and
      |    c_date is null and
      |    c_timestamp is not null""".stripMargin
  ) ++ castingExpressions

  for ((pE, i) <- pushableExpressions.zipWithIndex) {
    test(s"test_pushExpr_${i}", pE, () => unit_test_table.queryExecution.analyzed)
  }


}
