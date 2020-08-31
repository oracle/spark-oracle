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
package org.apache.spark.sql.sqlmacros

import org.apache.spark.sql.catalyst.{expressions => sparkexpr}
import org.apache.spark.sql.types.IntegralType

/**
 * Collapse [[sparkexpr.GetMapValue]], [[sparkexpr.GetStructField]] and
 * [[sparkexpr.GetArrayItem]] expressions. Also simplify `Unwrap <- Wrap` expression
 * sub-trees.
 */
trait ExprOptimize {
  self : ExprTranslator =>

  private def hasStaticKeys(m: sparkexpr.CreateMap): Boolean =
    m.keys.forall(_.isInstanceOf[sparkexpr.Literal])

  private def getValueExpr(
      m: sparkexpr.CreateMap,
      key: sparkexpr.Literal): sparkexpr.Expression = {
    var valExpr: sparkexpr.Expression = null

    for (i <- 0 until m.keys.size) {
      if (m.keys(i).canonicalized == key) {
        valExpr = m.values(i)
      }
    }

    if (valExpr == null) {
      valExpr = new sparkexpr.Literal(null, m.dataType.valueType)
    }
    valExpr
  }

  private def isValidField(s: sparkexpr.CreateNamedStruct, fIdx: Int): Boolean = {
    fIdx >= 0 && fIdx < s.dataType.fields.size
  }

  private def getFieldExpr(s: sparkexpr.CreateNamedStruct, fIdx: Int): sparkexpr.Expression = {
    s.valExprs(fIdx)
  }

  private def validIndex(a: sparkexpr.CreateArray, l: sparkexpr.Literal): Option[Int] = {
    if (l.dataType.isInstanceOf[IntegralType]) {
      val typ: IntegralType = l.dataType.asInstanceOf[IntegralType]
      val idx = typ.numeric.toInt(l.value.asInstanceOf[typ.InternalType])
      if (idx >= 0 && idx < a.children.size) {
        Some(idx)
      } else None
    } else None
  }

  private def geArrEntryExpr(a: sparkexpr.CreateArray, eIdx: Int): sparkexpr.Expression = {
    a.children(eIdx)
  }

  def optimizeExpr(expr: sparkexpr.Expression): sparkexpr.Expression = expr transformUp {
    case sparkexpr.objects.UnwrapOption(_, sparkexpr.objects.WrapOption(c, _)) => c
    case sparkexpr.GetMapValue(cm: sparkexpr.CreateMap, k: sparkexpr.Literal, false)
        if hasStaticKeys(cm) =>
      getValueExpr(cm, k)
    case e @ sparkexpr.GetStructField(s: sparkexpr.CreateNamedStruct, fIdx, _)
        if isValidField(s, fIdx) =>
      getFieldExpr(s, fIdx)
    case e @ sparkexpr.GetArrayItem(a: sparkexpr.CreateArray, ordExpr: sparkexpr.Literal, _) =>
      validIndex(a, ordExpr).map(geArrEntryExpr(a, _)).getOrElse(e)
    case e @ sparkexpr.GetMapValue(_: sparkexpr.Literal, _: sparkexpr.Literal, false) =>
      val value = e.eval(null)
      sparkexpr.Literal(value, e.dataType)
    case e @ sparkexpr.GetStructField(l: sparkexpr.Literal, fIdx, _) =>
      val value = e.eval(null)
      sparkexpr.Literal(value, e.dataType)
    case e @ sparkexpr.GetArrayItem(_: sparkexpr.Literal, _: sparkexpr.Literal, false) =>
      val value = e.eval(null)
      sparkexpr.Literal(value, e.dataType)
  }
}
