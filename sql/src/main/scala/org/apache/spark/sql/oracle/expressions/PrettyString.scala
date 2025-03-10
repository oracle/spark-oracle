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

import org.apache.spark.sql.catalyst.expressions.{Expression, ToPrettyString}
import org.apache.spark.sql.oracle.SQLSnippet

/**
 * Conversions for expressions in ''ToPrettyString.scala''
 */
object PrettyString {

   private case class OraPrettyString(catalystExpr: ToPrettyString, child: OraExpression)
     extends OraExpression {

     lazy val children: Seq[OraExpression] = Seq(child)

     def orasql: SQLSnippet = { toPrettyString }

    // Returns a pretty string conversion of a value.
    // At present it just avoids null value, instead provides string representation of 'NULL'
    private def toPrettyString: SQLSnippet = {
      if (child == null) {
        SQLSnippet.NULL_STRING
      } else {
        def castToString = SQLSnippet.TO_CHAR_PREFIX +
          child.orasql + SQLSnippet.RPAREN
        def cSnips = Seq(castToString, SQLSnippet.NULL_STRING)
        SQLSnippet.call(COALESCE, cSnips: _*)
      }
    }

    override def  withNewChildrenInternal(newChildren: IndexedSeq[OraExpression]):
    OraExpression = {
      super.legacyWithNewChildren(newChildren)
    }

  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ ToPrettyString(OraExpression(child), _) => OraPrettyString(cE, child)
      case _ => null
    })

}
