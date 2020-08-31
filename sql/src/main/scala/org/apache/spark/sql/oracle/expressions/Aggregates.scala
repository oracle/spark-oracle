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

package org.apache.spark.sql.oracle.expressions

import org.apache.spark.sql.catalyst.expressions.{CumeDist, DenseRank, Expression, NthValue, NTile, PercentRank, Rank, RowNumber}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.SQLSnippet.{comma, join, literalSnippet}

object Aggregates {

  case class OraFnWithNullHandling(fn : OraExpression,
                                   handleNulls: OraLiteralSql) extends OraExpression {
    val catalystExpr: Expression = fn.catalystExpr
    val children: Seq[OraExpression] = Seq(fn)
    override def orasql: SQLSnippet = osql"${fn} ${handleNulls}"

    def this(fn : OraExpression,
            ignoreNulls : Boolean) =
      this(fn,
        if (ignoreNulls) new OraLiteralSql(IGNORE_NULLS) else new OraLiteralSql(RESPECT_NULLS)
      )
  }

  case class OraAggDistinct(aggFnName : String,
                            catalystExpr: Expression,
                            children: Seq[OraExpression])
    extends OraExpression {
    val fnSnip = literalSnippet(aggFnName)
    private def args = children.map(_.orasql)
    override def orasql: SQLSnippet =
      osql"$fnSnip(DISTINCT ${join(args, comma, true)})"
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ NthValue(OraExpression(inOE), OraExpression(offsetOE), ignoreN) =>
        new OraFnWithNullHandling(OraBinaryFnExpression(NTH_VALUE, cE, inOE, offsetOE), ignoreN)
      case cE @ DenseRank(_) => OraNoArgFnExpression(DENSE_RANK, cE)
      case cE @ PercentRank(_) => OraNoArgFnExpression(PERCENT_RANK, cE)
      case cE @ Rank(_) => OraNoArgFnExpression(RANK, cE)
      case cE @ CumeDist() => OraNoArgFnExpression(CUME_DIST, cE)
      case cE @ NTile(OraExpression(bOE)) => OraUnaryFnExpression(NTILE, cE, bOE)
      case cE @ RowNumber() => OraNoArgFnExpression(ROW_NUMBER, cE)
      case cE @ Average(OraExpression(oE)) => OraUnaryFnExpression(AVG, cE, oE)
      case cE @ Sum(OraExpression(oE)) => OraUnaryFnExpression(SUM, cE, oE)
      case cE @ Count(OraExpressions(oEs @ _*)) => OraFnExpression(COUNT, cE, oEs)
      case cE @ Min(OraExpression(oE)) => OraUnaryFnExpression(MIN, cE, oE)
      case cE @ Max(OraExpression(oE)) => OraUnaryFnExpression(MAX, cE, oE)
      case cE @ First(OraExpression(oE), ignoreN) =>
        new OraFnWithNullHandling(OraUnaryFnExpression(FIRST_VALUE, cE, oE), ignoreN)
      case cE @ Last(OraExpression(oE), ignoreN) =>
        new OraFnWithNullHandling(OraUnaryFnExpression(LAST_VALUE, cE, oE), ignoreN)
        // for stddev, var, corvvar ignoring 'nullOnDivideByZero' arg.
      case cE @ StddevPop(OraExpression(oE), _) => OraUnaryFnExpression(STDDEV_POP, cE, oE)
      case cE @ StddevSamp(OraExpression(oE), _) => OraUnaryFnExpression(STDDEV_SAMP, cE, oE)
      case cE @ VariancePop(OraExpression(oE), _) => OraUnaryFnExpression(VAR_POP, cE, oE)
      case cE @ VarianceSamp(OraExpression(oE), _) => OraUnaryFnExpression(VAR_SAMP, cE, oE)
      case cE @ CovPopulation(OraExpression(lE), OraExpression(rE), _) =>
        OraBinaryFnExpression(COVAR_POP, cE, lE, rE)
      case cE @ CovSample(OraExpression(lE), OraExpression(rE), _) =>
        OraBinaryFnExpression(COVAR_SAMP, cE, lE, rE)
      case cE @ Corr(OraExpression(lE), OraExpression(rE), _) =>
        OraBinaryFnExpression(CORR, cE, lE, rE)
      case AggregateExpression(OraExpression(oE), Complete, false, None, _) => oE
      case AggregateExpression(OraExpression(oE), Complete, true, None, _) => oE match {
        case OraFnExpression(COUNT, cE, oEs) => OraAggDistinct(COUNT, cE, oEs)
        case OraFnExpression(SUM, cE, oEs) => OraAggDistinct(SUM, cE, oEs)
        case _ => null
      }
      case _ => null
    })

}
