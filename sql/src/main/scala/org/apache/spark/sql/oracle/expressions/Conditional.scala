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

import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Expression, If}
import org.apache.spark.sql.oracle.{OraSparkUtils, SQLSnippet}

/**
 * Conversions for expressions in ''conditionalExpressions.scala''
 */
object Conditional {

  case class OraSimpleCase(
      catalystExpr: If,
      cases: Seq[(OraExpression, OraExpression)],
      elseCase: Option[OraExpression])
      extends OraExpression {

    def orasql: SQLSnippet = SQLSnippet.simpleCase(
      cases.map(t => (t._1.orasql, t._2.orasql)),
      elseCase.map(_.orasql))

    override def children: Seq[OraExpression] =
      cases.flatMap(t => Seq(t._1, t._2)) ++ elseCase.toSeq
  }

  case class OraSearchedCase(
      catalystExpr: CaseWhen,
      branches: Seq[(OraExpression, OraExpression)],
      elseCase: Option[OraExpression])
      extends OraExpression {

    def orasql: SQLSnippet = SQLSnippet.searchedCase(
      branches.map(t => (t._1.orasql, t._2.orasql)),
      elseCase.map(_.orasql))

    override def children: Seq[OraExpression] =
      branches.flatMap(t => Seq(t._1, t._2)) ++ elseCase.toSeq
  }

  private object CaseBranch {
    def unapply(t: (Expression, Expression)): Option[(OraExpression, OraExpression)] =
      Option(t match {
        case (OraExpression(l), OraExpression(r)) => (l, r)
        case _ => null
      })
  }

  private object CaseBranches {
    def unapplySeq(
        eS: Seq[(Expression, Expression)]): Option[Seq[(OraExpression, OraExpression)]] =
      OraSparkUtils.sequence(eS.map(CaseBranch.unapply(_)))
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ If(OraExpression(condOE), OraExpression(trueOE), OraExpression(falseOE)) =>
        OraSimpleCase(cE, Seq((condOE, trueOE)), Some(falseOE))
      case cE @ CaseWhen(CaseBranches(branches @ _*), None) =>
        OraSearchedCase(cE, branches, None)
      case cE @ CaseWhen(CaseBranches(branches @ _*), Some(OraExpression(elseOE))) =>
        OraSearchedCase(cE, branches, Some(elseOE))
      case _ => null
    })
}
