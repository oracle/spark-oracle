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

import org.apache.spark.sql.catalyst.expressions.{Concat, Contains, EndsWith, Expression, Like, StartsWith, StringTrim, StringTrimLeft, StringTrimRight, Substring, Upper}
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.SQLSnippet.{comma, join, literalSnippet}

/**
 * Conversions for expressions in ''stringExpressions.scala''
 */

object Strings {

  case class OraStringTrim(catalystExpr: Expression,
                           trimType : SQLSnippet,
                           trimChar : Option[OraExpression],
                           trimSrc: OraExpression,
                          )
    extends OraExpression {

    val fnSnip = literalSnippet(TRIM)

    private def trimCharSnip = trimChar.map(_.orasql).getOrElse(SQLSnippet.empty)

    private def args = children.map(_.orasql)
    override def orasql: SQLSnippet =
      osql"$fnSnip(${trimType} ${trimCharSnip} FROM ${trimSrc})"

    override def children: Seq[OraExpression] = trimChar.toSeq ++ Seq(trimSrc)
  }

  private val TRIM_LEADING : SQLSnippet = literalSnippet(LEADING)
  private val TRIM_TRAILING : SQLSnippet = literalSnippet(TRAILING)
  private val TRIM_BOTH : SQLSnippet = literalSnippet(BOTH)

  case class OraLike(
                      catalystExpr: Expression,
                      char1: OraExpression,
                      char2: OraExpression,
                      esc: String,
                      isNot: Boolean = false)
    extends OraExpression {

    import SQLSnippet._

    lazy val orasql: SQLSnippet = if (isNot) {
      osql" (${char1} NOT LIKE ${char2} ESCAPE '${literalSnippet(esc)}') "
    } else {
      osql" (${char1} LIKE ${char2} ESCAPE '${literalSnippet(esc)}') "
    }

    override def children: Seq[OraExpression] = Seq(char1, char2)
  }

  case class OraContains(catalystExpr: Expression, child: OraExpression, pattern: String)
    extends OraExpression {

    import SQLSnippet._

    lazy val orasql: SQLSnippet =
      osql" (${child} LIKE '%${literalSnippet(pattern)}%') "

    override def children: Seq[OraExpression] = Seq(child)
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ Substring(OraExpression(s), OraExpression(pos), OraExpression(len)) =>
        OraFnExpression(SUBSTR, cE, Seq(s, pos, len))
      case cE @ Concat(OraExpressions(oEs @ _*)) =>
        oEs.reduceLeft { (conE : OraExpression, oE : OraExpression) =>
          OraBinaryFnExpression(CONCAT, cE, conE, oE)
        }
      case cE @ StartsWith(OraExpression(left), sE@OraExpression(right)) =>
        OraBinaryOpExpression(LIKE, cE, left,
          OraBinaryFnExpression(CONCAT, sE, right, new OraLiteralSql("'%'"))
        )
      case cE @ EndsWith(OraExpression(left), sE@OraExpression(right)) =>
        OraBinaryOpExpression(LIKE, cE, left,
          OraBinaryFnExpression(CONCAT, sE, new OraLiteralSql("'%'"), right)
        )
      case cl @ Like(OraExpression(left), OraExpression(right), escChar) =>
        OraLike(cl, left, right, escChar.toString)
      // catalyst LikeSimplification rule converts Like expr to a
      // Contains when pattern is a Literal. When generating ora-sql, generate the like expr
      case cE @ Contains(OraExpression(left), OraExpression(right))
        if right.isInstanceOf[OraLiteral] =>
        val pattern = right.asInstanceOf[OraLiteral].catalystExpr.value.toString
        OraContains(cE, left, pattern)
      case cE@Upper(OraExpression(oE)) => OraUnaryFnExpression(UPPER, cE, oE)
      case cE@StringTrimLeft(OraExpression(trimSrc), None) =>
        OraStringTrim(cE, TRIM_LEADING, None, trimSrc)
      case cE@StringTrimLeft(OraExpression(trimSrc), Some(OraExpression(trimChar))) =>
        OraStringTrim(cE, TRIM_LEADING, Some(trimChar), trimSrc)
      case cE@StringTrimRight(OraExpression(trimSrc), None) =>
        OraStringTrim(cE, TRIM_TRAILING, None, trimSrc)
      case cE@StringTrimRight(OraExpression(trimSrc), Some(OraExpression(trimChar))) =>
        OraStringTrim(cE, TRIM_TRAILING, Some(trimChar), trimSrc)
      case cE@StringTrim(OraExpression(trimSrc), None) =>
        OraStringTrim(cE, TRIM_BOTH, None, trimSrc)
      case cE@StringTrim(OraExpression(trimSrc), Some(OraExpression(trimChar))) =>
        OraStringTrim(cE, TRIM_BOTH, Some(trimChar), trimSrc)
      case _ => null
    })
}
