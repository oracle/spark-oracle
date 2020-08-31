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

package org.apache.spark.sql.oracle.sqlexec

import scala.util.control.NonFatal
import scala.util.parsing.combinator.JavaTokenParsers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.oracle.expressions.OraLiterals

/**
 * Represent a SQL Template of the form:
 * {{{
 * select * from user where id = {id} and user_name = {userName}
 * }}}
 * into:
 * {{{
 * SQLTemplate("select * from user where id = ? and user_name = ?", Seq("id", "name")
 * }}}
 * @param sql
 * @param paramNames
 */
case class SQLTemplate(sql: String, paramNames: Seq[String])

object SQLTemplate {

  def parse(templateSQL: String): SQLTemplate = {
    val sql = templateSQL.replaceAll("\\{.+?\\}", "?")
    val params = Parser.extractAllParameters(templateSQL)
    SQLTemplate(sql, params)
  }

  def buildPrintableSQL(sqlTemplate: String, params: Seq[Literal]): String =
    SQLTemplateFormatter.printableSQL(sqlTemplate, params)

  private object Parser extends JavaTokenParsers {

    def extractAllParameters(input: String): Seq[String] = {
      parse(mainParser, input).getOrElse(Nil)
    }

    private def mainParser: Parser[List[String]] = rep(name | other) ^^ { names =>
      names.collect { case name if name != "" => name }
    }

    private def name = "\\{\\w+\\}".r <~ opt(",") ^^ { name =>
      name.replaceFirst("\\{", "").replaceFirst("\\}", "").trim()
    }

    private def other = literal | token ^^ (_ => "")

    private def charLiteral = "'[^']*'".r ^^ (_ => "")

    private def sqlStringLiteral = ("\"[^(\")]*\"".r | super.stringLiteral) ^^ (_ => "")

    private def literal = (stringLiteral | charLiteral | floatingPointNumber) ^^ (_ => "")

    private def token = "[]\\w\\(\\)\\.\\-\\*&|!/=,<>%;`\\[\\]:]+".r ^^ (_ => "")
  }

  /*
   * based on PrintableQueryBuilder in scalikejdbc
   */
  private object SQLTemplateFormatter extends Logging {

    val substituteRegex = "(?<!\\?)(\\?)(?!\\?)".r

    def printableSQL(sqlTemplate: String, params: Seq[Literal]): String = {

      try {
        var i = 0

        sqlTemplate
          .split('\'')
          .zipWithIndex
          .map {
            // Even numbered parts are outside quotes, odd numbered are inside
            case (target, quoteCount) if (quoteCount % 2 == 0) =>
              substituteRegex.replaceAllIn(
                target,
                m => {
                  i += 1
                  if (params.size >= i) {
                    val lit = params(i - 1)
                    val parmSqlStr = OraLiterals
                      .toOraLiteralSql(lit)
                      .getOrElse(m.source.toString)
                    parmSqlStr
                  } else {
                    m.source.toString()
                  }
                })
            case (s, quoteCount) if (quoteCount % 2 == 1) =>
              // If the statement is valid, we can always expect an odd number of elements
              // Thus, we can add two quotes here.
              "'" + s + "'"
            case (s, _) => s
          }
          .mkString
      } catch {
        case NonFatal(e) =>
          logDebug(
            s"Failed to build a printable SQL statement from ${sqlTemplate}," +
              s" params: ${params}",
            e)
          sqlTemplate
      }
    }
  }
}
