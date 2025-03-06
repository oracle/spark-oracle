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

import org.apache.spark.sql.catalyst.expressions.{
  Abs,
  Add,
  Divide,
  Expression,
  Greatest,
  IntegralDivide,
  Least,
  Multiply,
  Pmod,
  Remainder,
  Subtract,
  UnaryMinus,
  UnaryPositive
}

/**
 * Conversions for expressions in ''arithmetic.scala''
 */
object Arithmetic {

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ UnaryMinus(OraExpression(oE), _) => OraUnaryOpExpression(MINUS, cE, oE)
      case cE @ UnaryPositive(OraExpression(oE)) => OraUnaryOpExpression(PLUS, cE, oE)
      case cE @ Abs(OraExpression(oE), _) => OraUnaryFnExpression(ABS, cE, oE)
      case cE @ Add(OraExpression(left), OraExpression(right), _) =>
        OraBinaryOpExpression(PLUS, cE, left, right)
      case cE @ Subtract(OraExpression(left), OraExpression(right), _) =>
        OraBinaryOpExpression(MINUS, cE, left, right)
      case cE @ Multiply(OraExpression(left), OraExpression(right), _) =>
        OraBinaryOpExpression(MULTIPLY, cE, left, right)
      case cE @ Divide(OraExpression(left), OraExpression(right), _) =>
        OraBinaryOpExpression(DIVIDE, cE, left, right)
      case cE @ IntegralDivide(OraExpression(left), OraExpression(right), _) =>
        OraUnaryFnExpression(TRUNC, cE, OraBinaryOpExpression(DIVIDE, cE, left, right))
      case cE @ Remainder(OraExpression(left), OraExpression(right), _) =>
        OraBinaryFnExpression(REMAINDER, cE, left, right)
      case cE @ Pmod(OraExpression(left), OraExpression(right), _) =>
        OraUnaryFnExpression(ABS, cE, OraBinaryFnExpression(MOD, cE, left, right))
      case cE @ Least(OraExpressions(oEs @ _*)) => OraFnExpression(LEAST, cE, oEs)
      case cE @ Greatest(OraExpressions(oEs @ _*)) => OraFnExpression(GREATEST, cE, oEs)
      case _ => null
    })

}
