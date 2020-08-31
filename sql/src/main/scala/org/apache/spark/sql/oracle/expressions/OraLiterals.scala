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

import java.sql._

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, LegacyDateFormats, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId
import org.apache.spark.sql.oracle.{OraSparkUtils, OraSQLLiteralBuilder, SQLSnippet}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class OraLiteral(catalystExpr: Literal) extends OraExpression with OraLeafExpression {
  override def orasql: SQLSnippet = osql"${catalystExpr}"

  def toLiteralSql: OraLiteralSql =
    new OraLiteralSql(OraLiterals.toOraLiteralSql(catalystExpr).get)

}

case class OraLiteralSql(catalystExpr: Literal) extends OraExpression with OraLeafExpression {
  assert(catalystExpr.dataType == StringType)
  override def orasql: SQLSnippet =
    SQLSnippet.literalSnippet(catalystExpr)

  def this(s: String) = this(Literal(UTF8String.fromString(s), StringType))
}

/**
 * There are 2 kinds of literal conversions:
 * - typically a literal is converted to a [[OraLiteral]], which encapsulates the
 *   spark literal and is set in the PreparedStatement as a bind value.
 * - sometimes you may need to convert the spark literal into a oracle literal value.
 *   This must be explicitly asked for by invoking [[OraLiterals.toOraLiteralSql]]
 *   Conversion is attempted using literal representation rules in the Oracle SQL Reference.
 */
object OraLiterals {

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case l: Literal if OraSQLLiteralBuilder.isSupportedForBind(l.dataType) =>
        OraLiteral(l)
      case _ => null
    })

  def toOraLiteralSql(l: Literal): Option[String] =
    ORA_LITERAL_CONV.ora_literal(l)

  /*
   * Based on https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Literals.html#GUID-192417E8-A79D-4A1D-9879-68272D925707
   *
   * Passed in values mustn't be null.
   *
   * Date Times:
   * Convert to oracle data and timestamp literals
   * - timestamp -> convert to a UTC timestamp. This works w/o knowledge of DB timeZone
   * - date -> convert to a UTC timestamp in oracle and then truncate.
   *   This works w/o knowledge of DB timeZone
   *
   * String: escape single "'" with "''"
   */
  private object ORA_LITERAL_CONV {

    val spk_dt_fmt = "yyyy-MM-dd"
    val ora_dt_format = "YYYY-MM-DD"

    val UTC = getZoneId("+00:00")
    val spk_ts_format = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    val spk_ts_fmtter =
      TimestampFormatter(
        spk_ts_format,
        UTC,
        LegacyDateFormats.FAST_DATE_FORMAT,
        isParsing = false)

    val ora_ts_format = "YYYY-MM-DD HH24:MI:SS:FF6 00:00"

    def to_ora_ts_literal(value: Long): String =
      s"TIMESTAMP '${spk_ts_fmtter.format(value)}'"

    def to_ora_dt_literal(value: Int): String = {
      val jvTS = new Timestamp(DateTimeUtils.toJavaDate(value).getTime)
      s"TRUNC(TIMESTAMP '${spk_ts_fmtter.format(jvTS)}')"
    }

    def to_string_literal(s: String): String = {
      def fn(s: String) =
        s.split("''")
          .map(_.replaceAll("'", "''"))
          .mkString("''")

      val r = s match {
        case "''" => "''"
        case _ => fn(s)
      }

      s"'${r}'"
    }

    /**
     * The [[Decimal]] value is converted to [[java.math.BigDecimal]]
     * Then [[java.math.BigDecimal:toString]] method is called.
     * This returns a string representation based on Decimal's precision and scale.
     *
     * @param d
     * @return
     */
    def to_decimal_literal(d: Decimal): String = d.toString

    def ora_literal(l: Literal): Option[String] =
      Option(l match {
        case Literal(null, _) => "null" // TODO: does this work in all cases?
        case Literal(s, StringType) => ORA_LITERAL_CONV.to_string_literal(s.toString)
        case Literal(d, DateType) => ORA_LITERAL_CONV.to_ora_dt_literal(d.asInstanceOf[Int])
        case Literal(t, TimestampType) => ORA_LITERAL_CONV.to_ora_ts_literal(t.asInstanceOf[Long])
        case Literal(n, _: IntegralType) => n.toString
        case Literal(f, FloatType) => s"${f}f"
        case Literal(f, DoubleType) => s"${f}d"
        case Literal(d: Decimal, dt: DecimalType) => to_decimal_literal(d)
        case _ => null
      })
  }

  def jdbcGetSet(dt: DataType): JDBCGetSet[_] = dt match {
    case StringType => StringGetSet
    case ByteType => ByteGetSet
    case ShortType => ShortGetSet
    case IntegerType => IntGetSet
    case LongType => LongGetSet
    case dt: DecimalType => new DecimalGetSet(dt)
    case FloatType => FloatGetSet
    case DoubleType => DoubleGetSet
    case DateType => DateGetSet
    case TimestampType => TimestampGetSet
    case st : StructType => StructGetSet(st)
    case at : ArrayType => ArrayGetSet(at)
    case mt : MapType => MapGetSet(mt)
    case _ =>
      OraSparkUtils.throwAnalysisException(
        s"Currently Unsupported DataType for reading/writing values from oracle: ${dt}")
  }

  def dataTypeMinMaxRange(dt : NumericType) : (OraExpression, OraExpression) = {
    val (minV, maxV) = dt match {
      case ByteType => (Literal(Byte.MinValue), Literal(Byte.MaxValue))
      case ShortType => (Literal(Short.MinValue), Literal(Short.MaxValue))
      case IntegerType => (Literal(Int.MinValue), Literal(Int.MaxValue))
      case LongType => (Literal(Long.MinValue), Literal(Long.MaxValue))
      case FloatType => (Literal(Float.MinValue), Literal(Float.MaxValue))
      case DoubleType => (Literal(Double.MinValue), Literal(Double.MaxValue))
      case dt : DecimalType =>
        val pVal = "9" * (dt.precision - dt.scale)
        val sVal = "9" * dt.scale
        val v = s"${pVal}.${sVal}"
        (s"-${v}", v)
    }
    (OraLiteral(Literal(minV)).toLiteralSql, OraLiteral(Literal(maxV)).toLiteralSql)
  }

  def bindValues(ps: PreparedStatement,
                         bindValues: Seq[Literal]): Unit = {
    val setters: Seq[JDBCGetSet[_]] =
      bindValues.map { lit =>
        jdbcGetSet(lit.dataType)
      }
    for (((bV, setter), i) <- bindValues.zip(setters).zipWithIndex) {
      setter.setValue(bV, ps, i)
    }
  }

}
