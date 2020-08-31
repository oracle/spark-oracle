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

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.oracle.SQLSnippet

/**
 * Conversions for expressions in ''predicates.scala''
 */
object Predicates {

  case class OraIn(catalystExpr: Predicate, oExpr: OraExpression, inList: Seq[OraExpression])
      extends OraExpression {

    import SQLSnippet._

    def orasql: SQLSnippet =
      oExpr.orasql + IN + LPAREN + csv(inList.map(_.orasql): _*) + RPAREN

    val children: Seq[OraExpression] = oExpr +: inList
  }

  case class OraInSubQuery(
      catalystExpr: InSubquery,
      inColumns: Seq[OraExpression],
      oraSubQry: OraExpression)
      extends OraExpression {
    import SQLSnippet._
    lazy val orasql: SQLSnippet =
      osql" (${csv(inColumns.map(_.orasql): _*)}) in ( ${oraSubQry.orasql} )"
    val children: Seq[OraExpression] = inColumns :+ oraSubQry
  }

  case class RownumLimit(op: SQLSnippet, limitVal: Long) extends OraExpression {
    val catalystExpr: Literal = Literal(limitVal)
    val oraExpr = OraLiteral(catalystExpr).toLiteralSql
    lazy val orasql: SQLSnippet = osql"rownum ${op} ${oraExpr}"
    val children: Seq[OraExpression] = Seq(oraExpr)
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cl @ Not(Like(OraExpression(left), OraExpression(right), escChar)) =>
        Strings.OraLike(cl, left, right, escChar.toString, true)
      case cE @ Not(OraExpression(oE)) => OraUnaryOpExpression(NOT, cE, oE)
      case cE @ InSubquery(OraExpressions(oEs @ _*), OraExpression(sQ)) =>
        OraInSubQuery(cE, oEs, sQ)
      case cE @ In(OraExpression(value), OraExpressions(oEs @ _*)) =>
        OraIn(cE, value, oEs)
      case cE @ InSet(OraExpression(value), hset) =>
        /*
         * For now try to convert the inList to a Literal list and then a
         * OE list
         */
        Try {
          val lits: Seq[Expression] =
            hset.map(Literal.fromObject(_, cE.child.dataType): Expression).toSeq
          OraExpressions
            .unapplySeq(lits)
            .map { inList =>
              OraIn(cE, value, inList)
            }
            .orNull
        }.getOrElse(null)
      case cE @ And(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(AND, cE, left, right)
      case cE @ Or(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(OR, cE, left, right)
      case cE @ EqualNullSafe(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(
          EQ,
          cE,
          OraFnExpression(
            DECODE,
            cE,
            Seq(
              left,
              right,
              OraLiteral(Literal(0)).toLiteralSql,
              OraLiteral(Literal(1)).toLiteralSql)),
          OraLiteral(Literal(0)).toLiteralSql)
      case cE @ BinaryComparison(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(cE.symbol, cE, left, right)
      case _ => null
    })
}
