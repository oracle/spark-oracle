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

import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, Expression, WindowExpression, WindowFrame, WindowSpecDefinition}
import org.apache.spark.sql.oracle.SQLSnippet

object Windows {

  case class OraWindowFrame(catalystExpr : WindowFrame,
                            orasql : SQLSnippet) extends OraExpression with OraLeafExpression

  case class OraWindowSpec(catalystExpr : WindowSpecDefinition,
                           partitionSpec: Seq[OraExpression],
                           orderSpec: Seq[OraExpression],
                           frameSpecification: OraWindowFrame) extends OraExpression {

    val children: Seq[OraExpression] =
      partitionSpec ++ orderSpec ++ Seq(frameSpecification)

    private def sqlSnip(oEs: Seq[OraExpression],
                        prefix: SQLSnippet,
                        suffix : SQLSnippet = SQLSnippet.empty): SQLSnippet = {

      if (oEs.isEmpty) {
        SQLSnippet.empty
      } else {
        osql"${prefix} ${SQLSnippet.csv(oEs.map(_.orasql) : _*)} ${suffix}"
      }

    }

    override def orasql: SQLSnippet = {
      val partSnippet = sqlSnip(partitionSpec, SQLSnippet.PARTITION_BY)
      val orderSnip = sqlSnip(orderSpec, SQLSnippet.ORDER_BY, frameSpecification.orasql)
      if (partitionSpec.isEmpty && orderSpec.isEmpty) {
        SQLSnippet.empty
      } else {
        osql"OVER ( ${partSnippet} ${orderSnip} )"
      }
    }

  }

  case class OraWindowExpression(catalystExpr : WindowExpression,
                                 windowFunction: OraExpression,
                                 windowSpec: OraWindowSpec) extends OraExpression {
    val children: Seq[OraExpression] = Seq(windowFunction, windowSpec)

    override def orasql: SQLSnippet = {
      osql"${windowFunction} ${windowSpec}"
    }
  }

  /**
   * for `NthVal, Rank, DenseRank, PercentRank, CumeDIst, NTile, RowNumber`
   * don't specify a Window. In Spark this is enforced by fixing the [[WindowFrame]]
   * for [[AggregateWindowFunction]] and then in [[ResolveWindowFrame]] rule
   * ensuring that if a frame is specified it matches the fixed [[WindowFrame]].
   *
   * @param wdwFunc
   * @param wf
   * @return
   */
  private def windowFrameSQLSnip(wdwFunc : OraExpression,
                                 wf : WindowFrame) : SQLSnippet = wdwFunc.catalystExpr match {
    case _ : AggregateWindowFunction => SQLSnippet.empty
    case _ => SQLSnippet.literalSnippet(wf.sql)
  }

  // scalastyle:off line.size.limit
  def unapply(e : Expression) : Option[OraExpression] = Option(x = e match {
    case wE@WindowExpression(OraExpression(windowFunction),
    ws@WindowSpecDefinition(OraExpressions(pSpecs @ _*), OraExpressions(oSpecs @ _*), wf : WindowFrame)
    ) =>
      OraWindowExpression(
        wE,
        windowFunction,
        OraWindowSpec(
          ws,
          pSpecs,
          oSpecs,
          OraWindowFrame(wf, windowFrameSQLSnip(windowFunction, wf))
        )
      )
    case _ => null
  })
  // scalastyle:on

}
