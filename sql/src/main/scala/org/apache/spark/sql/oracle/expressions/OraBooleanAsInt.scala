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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.oracle.SQLSnippet

/**
 * oracle sql doesn't allow boolean expressions as values.
 * So for example the following is illegal:
 * {{{
 * SELECT "C_INT" AS "val"
 * FROM SPARKTEST.UNIT_TEST
 * WHERE "C_INT" IS NOT NULL AND "C_INT" > 5 AND
 *    (COALESCE("C_INT", 0), "C_INT" IS NULL) IN
 *            (SELECT COALESCE("C_INT", 0), "C_INT" IS NULL
 *             FROM SPARKTEST.UNIT_TEST
 *             WHERE "C_INT" IS NOT NULL AND "C_INT" > 4
 *            )
 * }}}
 * because you cannot project `"C_INT" IS NULL` nor can you have
 * as a value.
 *
 * So we will convert boolean expressions into int values by the
 * following transform:
 * {{{
 *   "C_INT" IS NULL -> CASE WHEN "C_INT" IS NULL 1 ELSE 0 END
 * }}}
 */
case class OraBooleanAsInt(child : OraExpression) extends OraExpression {
  val catalystExpr: Expression = child.catalystExpr
  val children: Seq[OraExpression] = Seq(child)

  override def orasql: SQLSnippet =
    osql"CASE WHEN ${child} THEN 1 ELSE 0 END"
}
