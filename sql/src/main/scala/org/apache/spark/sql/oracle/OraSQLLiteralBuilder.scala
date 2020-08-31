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

package org.apache.spark.sql.oracle

import java.util.Date

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{
  DataType,
  DateType,
  Decimal,
  NumericType,
  StringType,
  TimestampType
}

trait OraSQLLiteralBuilder[T] {
  def apply(v: T): Literal
}

object OraSQLLiteralBuilder {

  abstract class LitConstruct[T] extends OraSQLLiteralBuilder[T] {
    override def apply(v: T): Literal = Literal.fromObject(v)
  }

  implicit object BooleanBuilder extends LitConstruct[Boolean]
  implicit object BooleanBoxedBuilder extends LitConstruct[java.lang.Boolean]

  implicit object ByteBuilder extends LitConstruct[Byte]
  implicit object ByteBoxedBuilder extends LitConstruct[java.lang.Byte]

  implicit object ShortBuilder extends LitConstruct[Short]
  implicit object ShortBoxedBuilder extends LitConstruct[java.lang.Short]

  implicit object IntBuilder extends LitConstruct[Int]
  implicit object IntBoxedBuilder extends LitConstruct[java.lang.Integer]

  implicit object LongBuilder extends LitConstruct[Long]
  implicit object LongBoxedBuilder extends LitConstruct[java.lang.Long]

  implicit object FloatBuilder extends LitConstruct[Float]
  implicit object FloatBoxedBuilder extends LitConstruct[java.lang.Float]

  implicit object DoubleBuilder extends LitConstruct[Double]
  implicit object DoubleBoxedBuilder extends LitConstruct[java.lang.Double]

  implicit object DecimalBuilder extends LitConstruct[Decimal]

  implicit object StringBuilder extends LitConstruct[String]

  implicit object DateBuilder extends LitConstruct[Date]

  implicit object LitLit extends OraSQLLiteralBuilder[Literal] {
    override def apply(v: Literal): Literal = v
  }

  implicit object SQLSnippetLit extends OraSQLLiteralBuilder[SQLSnippet] {
    override def apply(v: SQLSnippet): Literal = {
      // this should never be called
      UNSUPPORTED_ACTION("literal conversion", null, s"literal conversion of a SQLSnippet ${v}")
    }
  }

  def isSupportedForBind(sDT: DataType): Boolean = sDT match {
    case _: NumericType => true
    case StringType => true
    case TimestampType | DateType => true
    case _ => false
  }

  def toLiteral[T](v: T)(implicit lB: OraSQLLiteralBuilder[T]): Literal = lB(v)

  def literalOf(v: Any): Literal = {
    if (v == null) {
      UNSUPPORTED_ACTION("literal conversion", null, s"null value")
    } else {
      v match {
        case l: Literal =>
          if (!isSupportedForBind(l.dataType)) {
            UNSUPPORTED_ACTION(
              "literal conversion",
              l,
              s"unsupported datatype: ${l.dataType.toString}")
          }
          l
        case v: Boolean => toLiteral(v)
        case bv: java.lang.Boolean => toLiteral(bv)
        case v: Byte => toLiteral(v)
        case bv: java.lang.Byte => toLiteral(bv)
        case v: Short => toLiteral(v)
        case bv: java.lang.Short => toLiteral(bv)
        case v: Int => toLiteral(v)
        case bv: java.lang.Integer => toLiteral(bv)
        case v: Long => toLiteral(v)
        case bv: java.lang.Long => toLiteral(bv)
        case v: Float => toLiteral(v)
        case bv: java.lang.Float => toLiteral(bv)
        case v: Double => toLiteral(v)
        case bv: java.lang.Double => toLiteral(bv)
        case v: Decimal => toLiteral(v)
        case v: String => toLiteral(v)
        case v: Date => toLiteral(v)
        case _ =>
          UNSUPPORTED_ACTION(
            "literal conversion",
            null,
            s"unsupported literal value type: ${v.getClass.getName}")
      }
    }
  }

  private val UNSUPPORTED_ACTION = new UnSupportedActionHelper[Expression] {
    lazy val unsupportVerb: String = "Unsupported"
    lazy val actionKind: String = "conversion of ora-sql"
  }

}
