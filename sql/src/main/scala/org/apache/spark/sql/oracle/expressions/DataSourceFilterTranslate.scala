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

import org.apache.spark.sql.catalyst.{expressions => catexpr}
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.BooleanType

case class DataSourceFilterTranslate(fils : Seq[sources.Filter], oraTab : OraTable) {

  private lazy val schema = oraTab.catalystSchema

  def oraExpression : OraExpression = {
    val oraExprO = catalystExpr.flatMap(OraExpression.unapply)

    if (!oraExprO.isDefined) {
      throw new UnsupportedOperationException(
        s"""Delete condition filters: ${fils.mkString("[", ",", "]")}
           |cannot translate to oracle delete expression""".stripMargin
      )
    }
    oraExprO.get
  }

  private object LiteralVal {
    def unapply(value : Any) : Option[catexpr.Literal] =
      scala.util.Try(catexpr.Literal(value)).toOption
  }

  private object LiteralVals {
    def unapply(eS: Array[Any]): Option[Seq[catexpr.Literal]] =
      OraSparkUtils.sequence(eS.map(LiteralVal.unapply(_)))
  }

  private object AttrRef {
    def unapply(attr : String) : Option[catexpr.AttributeReference] =
      for (i <- schema.getFieldIndex(attr)) yield {
        val field = schema(i)
        catexpr.AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
      }
  }

  private object CatalystExpr {
    def unapply(f : sources.Filter) : Option[catexpr.Expression] = Option(
      f match {
      case sources.And(CatalystExpr(l), CatalystExpr(r)) => catexpr.And(l, r)
      case sources.Or(CatalystExpr(l), CatalystExpr(r)) => catexpr.Or(l, r)
      case sources.Not(CatalystExpr(c)) => catexpr.Not(c)
      case sources.EqualTo(AttrRef(c), LiteralVal(l)) => catexpr.EqualTo(c, l)
      case sources.EqualNullSafe(AttrRef(c), LiteralVal(l)) => catexpr.EqualNullSafe(c, l)
      case sources.IsNull(AttrRef(c)) => catexpr.IsNull(c)
      case sources.IsNotNull(AttrRef(c)) => catexpr.IsNotNull(c)
      case sources.In(AttrRef(c), LiteralVals(l)) => catexpr.In(c, l)
      case sources.GreaterThan(AttrRef(c), LiteralVal(l)) => catexpr.GreaterThan(c, l)
      case sources.GreaterThanOrEqual(AttrRef(c), LiteralVal(l)) =>
        catexpr.GreaterThanOrEqual(c, l)
      case sources.LessThan(AttrRef(c), LiteralVal(l)) => catexpr.LessThan(c, l)
      case sources.LessThanOrEqual(AttrRef(c), LiteralVal(l)) => catexpr.LessThanOrEqual(c, l)
      case sources.StringContains(AttrRef(c), LiteralVal(l)) => catexpr.Contains(c, l)
      case sources.StringStartsWith(AttrRef(c), LiteralVal(l)) => catexpr.StartsWith(c, l)
      case sources.StringEndsWith(AttrRef(c), LiteralVal(l)) => catexpr.EndsWith(c, l)
      case sources.AlwaysTrue => catexpr.Literal(true, BooleanType)
      case sources.AlwaysFalse => catexpr.Literal(false, BooleanType)
      case _ => null
    })
  }


  private def catalystExpr : Option[catexpr.Expression] = {
    val catExprs = OraSparkUtils.sequence(fils.map(CatalystExpr.unapply(_)))
    catExprs.map(cEs => cEs.reduceLeft(catexpr.And))
  }

}
