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

package org.apache.spark.sql.connector.catalog.oracle

import java.sql.{JDBCType, Types}

import org.apache.spark.sql.{types, SparkSession}
import org.apache.spark.sql.oracle.{OraSparkConfig, OraSparkUtils}
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NumericType, ShortType, StringType, StructField, StructType, TimestampType}

/**
 * References:
 * - https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
 * - https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/accessing-and-manipulating-Oracle-data.html#GUID-231E827F-77EE-4648-B0C4-98651F9CE03F
 * - [[org.apache.spark.sql.execution.datasources.jdbc]] package
 */
trait OraDataType {
  def sqlType: Int
  def catalystType: DataType

  def oraTypeString: String
}

case class OraChar(size: Int, inChars: Boolean) extends OraDataType {
  @transient lazy val sqlType: Int = Types.CHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = s"CHAR(${size}${if (!inChars) "BYTE" else ""})"
}

case class OraVarchar2(size: Int, inChars: Boolean) extends OraDataType {
  @transient lazy val sqlType: Int = Types.VARCHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = s"VARCHAR(${size}${if (!inChars) "BYTE" else ""})"
}

case class OraNChar(size: Int) extends OraDataType {
  @transient lazy val sqlType: Int = Types.NCHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = s"NCHAR(${size})"
}

case class OraNVarchar2(size: Int) extends OraDataType {
  @transient lazy val sqlType: Int = Types.NVARCHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = s"NVARCHAR2(${size})"
}

case class OraNumber(precision: Option[Int], scale: Option[Int]) extends OraDataType {

  OraNumber.checkSupported(precision, scale)

  private def precisionToSQLType(precision: Int): Int =
    if (precision <= 2) {
      Types.TINYINT
    } else if (precision <= 4) {
      Types.SMALLINT
    } else if (precision <= 9) {
      Types.INTEGER
    } else if (precision <= 18) {
      Types.BIGINT
    } else {
      Types.BIGINT
    }

  @transient lazy val sqlType: Int = ((precision, scale): @unchecked) match {
    case (Some(p), Some(0)) => precisionToSQLType(p)
    case (Some(p), None) => precisionToSQLType(p)
    case (Some(_), _) => Types.NUMERIC
    case (None, Some(_)) => Types.NUMERIC
    case (None, None) => Types.NUMERIC
  }

  @transient lazy val catalystType: DataType =
    OraDataType.toCatalystType(sqlType, precision, scale)

  @transient lazy val oraTypeString: String = OraNumber.toOraTypeNm(precision, scale)
}

object OraNumber {
  import OracleMetadata.checkDataType

  def toOraTypeNm(precision: Option[Int], scale: Option[Int]): String =
    ((precision, scale): @unchecked) match {
      case (None, None) => "NUMBER"
      case (None, Some(0)) => "NUMBER"
      case (Some(p), None) => s"NUMBER(${p})"
      case (Some(p), Some(s)) => if (s > 0) s"NUMBER(${p}, ${s})" else (s"NUMBER(${p})")
    }

  // Precision Defined && Scale Defined -> Check if Scale is smaller that precision.
  // Precision Not Defined && Scale Not Defined -> Allowed (NUMBER)
  // Precision Not Defined && Scale Defined -> Not Allowed.
  // Precision Defined && Scale Not Defined -> Allowed.
  def checkSupported(precision: Option[Int], scale: Option[Int]): Unit = {
    if (!precision.isDefined) {
      checkDataType(
        scale.isDefined && scale.get != 0,
        toOraTypeNm(precision, scale),
        s"scale cannot be defined if precision is undefined")
    } else {
      if (scale.isDefined) {
        val p = precision.get
        val s = scale.get
        checkDataType(s < 0, toOraTypeNm(precision, scale), "scale cannot be negative")
        checkDataType(
          s > p,
          toOraTypeNm(precision, scale),
          "scale cannot be greater than precision")
      }
    }
  }
}

case class OraFloat(precision: Option[Int]) extends OraDataType {
  @transient lazy val sqlType: Int = precision match {
    case None => Types.NUMERIC
    case Some(p) if p <= 7 => Types.FLOAT
    case Some(p) if p <= 15 => Types.DOUBLE
    case _ => Types.NUMERIC
  }

  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType, precision)
  @transient lazy val oraTypeString: String =
    s"FLOAT${if (precision.isDefined) "(" + precision.get + ")" else ""}"
}

case object OraLong extends OraDataType {
  @transient lazy val sqlType: Int = Types.LONGNVARCHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = "LONG"
}

case object OraBinaryFloat extends OraDataType {
  @transient lazy val sqlType: Int = Types.FLOAT
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = "BINARY_FLOAT"
}

case object OraBinaryDouble extends OraDataType {
  @transient lazy val sqlType: Int = Types.DOUBLE
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = "BINARY_DOUBLE"
}

case object OraDate extends OraDataType {
  @transient lazy val sqlType: Int = Types.DATE
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = "DATE"
}

trait OraTimestampTypes extends OraDataType {
  def frac_secs_prec: Option[Int]
}

case class OraTimestamp(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  @transient lazy val sqlType: Int = Types.TIMESTAMP
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String =
    s"TIMESTAMP${if (frac_secs_prec.isDefined) "(" + frac_secs_prec.get + ")" else ""}"
}

case class OraTimestampWithTZ(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  @transient lazy val sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String =
    s"TIMESTAMP${if (frac_secs_prec.isDefined) "(" + frac_secs_prec.get + ")" else ""}" +
      s"WITH TIME ZONE"
}

case class OraTimestampWithLocalTZ(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  @transient lazy val sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String =
    s"TIMESTAMP${if (frac_secs_prec.isDefined) "(" + frac_secs_prec.get + ")" else ""}" +
      s"WITH LOCAL TIME ZONE"
}

trait OraUDT extends OraDataType {
  val schema : String
  val oraTypName : String

  lazy val oraTypeString: String = s"${schema}.${oraTypName}"
}

case class OraStructField(name : String, oraDT : OraDataType)

case class OraStructType(schema : String,
                         oraTypName : String,
                         fields : Array[OraStructField]) extends OraUDT {
  val sqlType: Int = Types.STRUCT
  lazy val catalystType: DataType =
    StructType(fields.map(f => StructField(f.name, f.oraDT.catalystType)))
}

case class OraArrayType(schema : String,
                        oraTypName : String,
                        elemType : OraDataType,
                        limit : Int) extends OraUDT {
  val sqlType: Int = Types.ARRAY
  lazy val catalystType: DataType = ArrayType(elemType.catalystType)
}

case class OraNestedTableType(schema : String,
                              oraTypName : String,
                              valueType : OraDataType) extends OraUDT {
  val sqlType: Int = Types.ARRAY
  lazy val catalystType: DataType = MapType(IntegerType, valueType.catalystType)
}

object OraDataType {

  def create(
      s: String,
      length: Option[Int],
      precision: Option[Int],
      scale: Option[Int]): OraDataType =
    OracleCatalog.oracleCatalog.getMetadataManager.createOraDataType(s, length, precision, scale)

  def toCatalystType(
      sqlType: Int,
      precision: Option[Int] = None,
      scale: Option[Int] = None): DataType = (sqlType, precision, scale) match {
    case (Types.CHAR, _, _) => StringType
    case (Types.VARCHAR, _, _) => StringType
    case (Types.NCHAR, _, _) => StringType
    case (Types.NVARCHAR, _, _) => StringType
    case (Types.TINYINT, _, _) => ByteType
    case (Types.SMALLINT, _, _) => ShortType
    case (Types.INTEGER, _, _) => IntegerType
    case (Types.BIGINT, Some(p), _) if p <= 18 => LongType
    case (Types.BIGINT, Some(p), _) => DecimalType(p, 0)
    case (Types.NUMERIC, Some(p), Some(s)) if p < DecimalType.MAX_PRECISION => DecimalType(p, s)
    case (Types.NUMERIC, _, _) => DecimalType.SYSTEM_DEFAULT
    case (Types.FLOAT, _, _) => FloatType
    case (Types.DOUBLE, _, _) => DoubleType
    case (Types.LONGNVARCHAR, _, _) => StringType
    case (Types.DATE, _, _) => DateType
    case (Types.TIMESTAMP, _, _) => TimestampType
    case (Types.TIMESTAMP_WITH_TIMEZONE, _, _) => TimestampType
    case _ =>
      OraSparkUtils.throwAnalysisException(
        s"Unsupported SqlType: " +
          s"${JDBCType.valueOf(sqlType).getName}")
  }

  def toOraDataType(dataType: DataType)
                   (implicit sparkSession : SparkSession = OraSparkUtils.currentSparkSession)
  : OraDataType = dataType match {
    case BooleanType => OraNumber(Some(1), None)
    case ByteType => OraNumber(Some(3), None)
    case ShortType => OraNumber(Some(5), None)
    case IntegerType => OraNumber(Some(10), None)
    case LongType => OraNumber(Some(19), None)
    // based on [[DecimalType#FloatDecimal]] definition
    case FloatType => OraNumber(Some(14), Some(7))
    // based on [[DecimalType#DoubleDecimal]] definition
    case DoubleType => OraNumber(Some(30), Some(15))
    case decT : DecimalType => OraNumber(Some(decT.precision), Some(decT.scale))
    case StringType => OraVarchar2(OraSparkConfig.getConf(OraSparkConfig.VARCHAR2_MAX_LENGTH), true)
    case DateType => OraDate
    case types.TimestampType => OraTimestamp(None)
    case _ => OraSparkUtils.throwAnalysisException(
      s"Unsupported dataType translation: ${dataType}")
  }

  def dataTypeName(datatype : String,
                   udt_owner : Option[String],
                   udt_typename : Option[String]) : String = {
    // if (Set("UDT", "NESTED_TABLE", "VARRAY").contains( datatype.toUpperCase())) {
    if (udt_owner.isDefined && udt_typename.isDefined) {
      s"${udt_owner.get}.${udt_typename.get}"
    } else datatype
  }
}
