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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{CheckOverflow, Expression, PromotePrecision}
import org.apache.spark.sql.oracle.OraSQLImplicits

/**
 *  1. Currently just drop the [[CheckOverflow]] check.
 *     We handle [[CheckOverflow]] if it is on top of a [[Cast]].
 *     See [[Casts.unapply()]]
 *  2. When evaluating [[PromotePrecision]] is just a pass-through in Spark SQL.
 */
object IgnoreExpressions extends OraSQLImplicits with Logging {

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case co@CheckOverflow(OraExpression(oE), _, _) =>
        logWarning(
          s"""Ignoring checkoverflow when translating to oracle sql:
             |  for expression: ${co}""".stripMargin
        )
        oE
      case PromotePrecision(OraExpression(oE)) =>
        oE
      case _ => null
    })
}
