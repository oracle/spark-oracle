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

import java.math.BigDecimal
import java.sql._

import scala.collection.mutable.ArrayBuffer

import oracle.jdbc.OracleConnection
import oracle.sql.{CHAR, DATE, Datum, NUMBER, TIMESTAMP}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, GenericArrayData, MapData}
import org.apache.spark.sql.connector.catalog.oracle.{OraArrayType, OraDataType, OraStructType, OraUDT}
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraColumn
import org.apache.spark.sql.oracle.expressions.OraLiterals.jdbcGetSet
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * '''Reading and Writing Oracle UDT(STruct, Varray, NestedTable):'''
 *
 * '''Read Flow:'''
 *  - invoke [[ResultSet]] `getObject` and cast value as a [[Datum]]
 *  - invoke `readDatum` on the [[Datum]] value.
 *  - readDatum ''Structs:''
 *   - construct an [[InternalRow]]
 *   - iterate over field datums(obtained by calling `oraStruct.getOracleAttributes`)
 *   - call `fieldGetSet.setIRow(iRow, i, datum)`
 *   - `setIRow` recursively calls `readDatum` on  field's [[Datum]] value and calls
 *      `setIRow(oRow, pos, readDatum(datum))`
 *  - readDatum ''Varrays:'' and ''NestedTable'':
 *    - construct a [[scala.Array]]
 *    - iterate over elem datums(obtained by calling `oraArray.getOracleArray`)
 *    - `readDatum(datum)` into an intermediate `InternalRow` by calling
 *       `setIRow(oRow: InternalRow, pos: Int, datum : Datum)` and then return the value
 *       in the intermediate `InternalRow`.
 *
 * '''Write Flow:'''
 *  - call `ps.setObject(pos, constructJdbcObject(ps, v, oraTableShape(pos - 1).dataType))`
 *  - constructJdbcObject ''Structs:''
 *   - for each field in the Struct get its value from the [[InternalRow]] by calling
 *     `fGS.readIRow(iRow, i)`
 *   - if the field is a [[UDTypeGetSet]] recursively call
 *     `uGS.constructJdbcObject(ps, tVal, oraFld.oraDT)`
 *   - set the field value in an Array.
 *   - Finally construct a jdbc struct:
 *     `ps.getConnection.createStruct(oraDT.oraTypeString, oraObjArr)`
 *  - constructJdbcObject ''Varrays:'' and ''NestedTable'':
 *    - for each elem in the array, put it in an 1 field intermediate [[InternalRow]]
 *      and get its value by calling `componentGetSet.readIRow(iRow, i)`
 *    - if the field is a [[UDTypeGetSet[_]] recursively call
 *      `uGS.constructJdbcObject(ps, tVal, oraFld.oraDT)`
 *    - Finally construct a jdbc array:
 *      `ps.getConnection.asInstanceOf[OracleConnection].createARRAY(oraDT.oraTypeString, oraObjArr)`
 * @tparam T
 */
sealed trait JDBCGetSet[T] {

  val sqlType: Int

  protected def readResultSet(rs: ResultSet, pos: Int): T

  def readValue(rs: ResultSet, pos: Int) : T =
    readResultSet(rs, pos)
  def readOptionValue(rs: ResultSet, pos: Int) : Option[T] = {
    val v : T = readResultSet(rs, pos)
    if (rs.wasNull()) None else Some(v)
  }

  protected def setIRow(oRow: InternalRow, pos: Int, v: T): Unit
  def readIRow(iRow: InternalRow, pos: Int): T
  protected def readLiteral(lit: Literal): T
  protected def setPrepStat(ps: PreparedStatement,
                            pos: Int,
                            v: T,
                            oraTableShape : scala.Array[OraColumn]): Unit

  def readDatum(datum : Datum) : T
  final def setIRow(oRow: InternalRow, pos: Int, datum : Datum) : Unit = {
    if (datum.isNull && !this.isInstanceOf[UDTypeGetSet[_]]) {
      oRow.setNullAt(pos)
    } else {
      setIRow(oRow, pos, readDatum(datum))
    }
  }

  /**
   * Read a value from the [[ResultSet]] and set it in the [[InternalRow]]
   * @param rs
   * @param pos
   * @param row
   */
  final def readValue(rs: ResultSet, pos: Int, row: InternalRow): Unit = {
    val v: T = readResultSet(rs, pos + 1)
    if (rs.wasNull()) {
      row.setNullAt(pos)
    } else {
      setIRow(row, pos, v)
    }
  }

  /**
   * Read a value from the [[InternalRow]] and set it in the [[PreparedStatement]]
   * @param row
   * @param ps
   * @param pos
   */
  final def setValue(row: InternalRow,
                     ps: PreparedStatement,
                     pos: Int,
                     oraTableShape : scala.Array[OraColumn]): Unit = {
    if (row.isNullAt(pos)) {
      if (oraTableShape(pos).dataType.isInstanceOf[OraUDT]) {
        ps.setNull(pos + 1, sqlType, oraTableShape(pos).dataType.oraTypeString)
      } else {
        ps.setNull(pos + 1, sqlType)
      }
    } else {
      val v: T = readIRow(row, pos)
      setPrepStat(ps, pos + 1, v, oraTableShape)
    }
  }

  final def setValue(lit: Literal, ps: PreparedStatement, pos: Int): Unit = {
    if (lit.value == null) {
      ps.setNull(pos + 1, sqlType)
    } else {
      val v: T = readLiteral(lit)
      setPrepStat(ps, pos + 1, v, null)
    }
  }

  def toDatum(value : T) : Datum
  final def toDatum(lit : Literal) : Datum = {
    toDatum(readLiteral(lit))
  }
}

private object StringGetSet extends JDBCGetSet[String] {
  override val sqlType: Int = Types.VARCHAR

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getString(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: String) =
    iRow.update(pos, UTF8String.fromString(v))

  @inline def readIRow(iRow: InternalRow, pos: Int) : String = iRow.getString(pos)
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int, v: String,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setString(pos, v)

  @inline def readDatum(datum : Datum) : String = datum.stringValue()
  @inline override protected def readLiteral(lit: Literal): String =
    lit.value.asInstanceOf[UTF8String].toString

  @inline def toDatum(value : String) : Datum = new CHAR(value, null)
}

private object ByteGetSet extends JDBCGetSet[Byte] {
  override val sqlType: Int = Types.TINYINT

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getByte(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Byte) =
    iRow.setByte(pos, v)

  @inline def readIRow(iRow: InternalRow, pos: Int) : Byte = iRow.getByte(pos)
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: Byte,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setByte(pos, v)
  @inline def readDatum(datum : Datum) : Byte = datum.byteValue()
  @inline override protected def readLiteral(lit: Literal): Byte =
    lit.value.asInstanceOf[Byte]

  @inline def toDatum(value : Byte) : Datum = new NUMBER(value)
}

private object ShortGetSet extends JDBCGetSet[Short] {
  override val sqlType: Int = Types.SMALLINT

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getShort(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Short) =
    iRow.setShort(pos, v)

  @inline def readIRow(iRow: InternalRow, pos: Int) : Short = iRow.getShort(pos)
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: Short,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setShort(pos, v)
  @inline def readDatum(datum : Datum) : Short = datum.intValue().toShort
  @inline override protected def readLiteral(lit: Literal): Short =
    lit.value.asInstanceOf[Short]

  @inline def toDatum(value : Short) : Datum = new NUMBER(value)
}

private object IntGetSet extends JDBCGetSet[Int] {
  override val sqlType: Int = Types.INTEGER

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getInt(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Int) =
    iRow.setInt(pos, v)

  @inline def readIRow(iRow: InternalRow, pos: Int) : Int = iRow.getInt(pos)
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: Int,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setInt(pos, v)
  @inline def readDatum(datum : Datum) : Int = datum.intValue()
  @inline override protected def readLiteral(lit: Literal): Int =
    lit.value.asInstanceOf[Int]

  @inline def toDatum(value : Int) : Datum = new NUMBER(value)
}

private object LongGetSet extends JDBCGetSet[Long] {
  override val sqlType: Int = Types.BIGINT

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getLong(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Long) =
    iRow.setLong(pos, v)

  @inline def readIRow(iRow: InternalRow, pos: Int) : Long = iRow.getLong(pos)
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: Long,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setLong(pos, v)
  @inline def readDatum(datum : Datum) : Long = datum.longValue()
  @inline override protected def readLiteral(lit: Literal): Long =
    lit.value.asInstanceOf[Long]

  @inline def toDatum(value : Long) : Datum = new NUMBER(value)
}

private class DecimalGetSet(val dt: DecimalType) extends JDBCGetSet[BigDecimal] {
  override val sqlType: Int = Types.NUMERIC

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getBigDecimal(pos)

  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: BigDecimal) =
    iRow.update(pos, Decimal(v, dt.precision, dt.scale))

  @inline def readIRow(iRow: InternalRow, pos: Int) : BigDecimal =
    iRow.getDecimal(pos, dt.precision, dt.scale).toJavaBigDecimal

  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: BigDecimal,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setBigDecimal(pos, v)
  @inline def readDatum(datum : Datum) : BigDecimal = datum.bigDecimalValue()
  @inline override protected def readLiteral(lit: Literal): BigDecimal =
    lit.value.asInstanceOf[Decimal].toJavaBigDecimal

  @inline def toDatum(value : BigDecimal) : Datum = new NUMBER(value)
}

private object FloatGetSet extends JDBCGetSet[Float] {
  override val sqlType: Int = Types.FLOAT

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getFloat(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Float) =
    iRow.setFloat(pos, v)

  @inline def readIRow(iRow: InternalRow, pos: Int) : Float = iRow.getFloat(pos)
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: Float,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setFloat(pos, v)
  @inline def readDatum(datum : Datum) : Float = datum.floatValue()
  @inline override protected def readLiteral(lit: Literal): Float =
    lit.value.asInstanceOf[Float]

  @inline def toDatum(value : Float) : Datum = new NUMBER(value)
}

private object DoubleGetSet extends JDBCGetSet[Double] {
  override val sqlType: Int = Types.DOUBLE

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getDouble(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Double) =
    iRow.setDouble(pos, v)

  @inline def readIRow(iRow: InternalRow, pos: Int) : Double = iRow.getDouble(pos)
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: Double,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setDouble(pos, v)
  @inline def readDatum(datum : Datum) : Double = datum.doubleValue()
  @inline override protected def readLiteral(lit: Literal): Double =
    lit.value.asInstanceOf[Double]

  @inline def toDatum(value : Double) : Datum = new NUMBER(value)
}

private object DateGetSet extends JDBCGetSet[Date] {
  override val sqlType: Int = Types.DATE

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getDate(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Date) =
    iRow.setInt(pos, DateTimeUtils.fromJavaDate(v))

  @inline def readIRow(iRow: InternalRow, pos: Int) : Date =
    DateTimeUtils.toJavaDate(iRow.getInt(pos))
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: Date,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setDate(pos, v)
  @inline def readDatum(datum : Datum) : Date = datum.dateValue()
  @inline override protected def readLiteral(lit: Literal): Date =
    DateTimeUtils.toJavaDate(lit.value.asInstanceOf[Int])

  @inline def toDatum(value : Date) : Datum = new DATE(value)
}

private object TimestampGetSet extends JDBCGetSet[Timestamp] {
  override val sqlType: Int = Types.TIMESTAMP

  @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getTimestamp(pos)
  @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Timestamp) =
    iRow.setLong(pos, DateTimeUtils.fromJavaTimestamp(v))

  @inline def readIRow(iRow: InternalRow, pos: Int) : Timestamp =
    DateTimeUtils.toJavaTimestamp(iRow.getLong(pos))
  @inline protected def setPrepStat(ps: PreparedStatement,
                                    pos: Int,
                                    v: Timestamp,
                                    oraTableShape : scala.Array[OraColumn]) =
    ps.setTimestamp(pos, v)
  @inline def readDatum(datum : Datum) : Timestamp = datum.timestampValue()
  @inline override protected def readLiteral(lit: Literal): Timestamp =
    DateTimeUtils.toJavaTimestamp(lit.value.asInstanceOf[Long])

  @inline def toDatum(value : Timestamp) : Datum = new TIMESTAMP(value)
}

trait UDTypeGetSet[T] extends JDBCGetSet[T] {
  def constructJdbcObject(ps : PreparedStatement,
                          v : AnyRef,
                          oraDT : OraDataType
                         ) : AnyRef

  def toDatum(value : T) : Datum =
    throw new UnsupportedOperationException("convert Literal to Datum on a UDT")
}

object UDTypeGetSet {

  case class ConvertToCatalyst(componentGetSet : JDBCGetSet[_]) {
    /*
     * iRow: holds 1 elem of the Array
     * rowArr: is Array of the iRow
     * For each elem in Arr
     *   copy elem into iRow.rowArr
     *   call componentGetSet.readIRow on iRow
     */
    val iRowArr = scala.Array.fill[Any](1)(null)
    val iRow = new GenericInternalRow(iRowArr)

    def getJdbcVal(elemVal : Any) : AnyRef = {
      iRowArr(0) = elemVal
      componentGetSet.readIRow(iRow, 0).asInstanceOf[AnyRef]
    }

    def getCatalystVal(jdbcVal : Datum) : Any = {
      componentGetSet.setIRow(iRow, 0, jdbcVal)
      iRowArr(0)
    }
  }
}

private case class StructGetSet(dt : StructType)
  extends UDTypeGetSet[InternalRow] {

  private val fieldGetSets : scala.Array[JDBCGetSet[_]] =
    for (dtF <- dt.fields) yield {
      jdbcGetSet(dtF.dataType)
    }

  override val sqlType: Int = Types.STRUCT

  def readDatum(datum : Datum) : InternalRow = {
    if (datum == null) {
      null
    } else {
      val oraStruct = datum.asInstanceOf[oracle.sql.STRUCT]
      val datums = oraStruct.getOracleAttributes
      val iRow = new GenericInternalRow(scala.Array.fill[Any](dt.size)(null))
      for (((f, d), i) <- fieldGetSets.zip(datums).zipWithIndex) {
        if (d != null) {
          f.setIRow(iRow, i, d)
        }
      }
      iRow
    }
  }

  override protected def readResultSet(rs: ResultSet, pos: Int): InternalRow = {
    readDatum(rs.getObject(pos).asInstanceOf[Datum])
  }

  override protected def setIRow(oRow: InternalRow, pos: Int, v: InternalRow): Unit = {
    oRow(pos) = v
  }

  override def readIRow(iRow: InternalRow, pos: Int): InternalRow = {
    iRow.get(pos, dt).asInstanceOf[InternalRow]
  }

  override protected def readLiteral(lit: Literal): InternalRow = {
    lit.value.asInstanceOf[InternalRow]
  }

  def constructJdbcObject(ps : PreparedStatement,
                          v : AnyRef,
                          oraDT : OraDataType
                         ): AnyRef = {
    val oraStructDT = oraDT.asInstanceOf[OraStructType]
    val iRow = v.asInstanceOf[InternalRow]

    val oraObjArr = scala.Array.fill[AnyRef](fieldGetSets.size)(null)
    for(((fGS, oraFld), i) <- fieldGetSets.zip(oraStructDT.fields).zipWithIndex) {
      if (iRow.isNullAt(i)) {
        oraObjArr(i) = null
      } else {
        val tVal = fGS.readIRow(iRow, i).asInstanceOf[AnyRef]
        fGS match {
          case uGS : UDTypeGetSet[_] =>
            oraObjArr(i) = uGS.constructJdbcObject(ps, tVal, oraFld.oraDT)
          case _ =>
            oraObjArr(i) = tVal
        }
      }
    }
    ps.getConnection.createStruct(oraDT.oraTypeString, oraObjArr)
  }

  override protected def setPrepStat(ps: PreparedStatement,
                                     pos: Int,
                                     v: InternalRow,
                                     oraTableShape : scala.Array[OraColumn]): Unit = {
    ps.setObject(pos, constructJdbcObject(ps, v, oraTableShape(pos - 1).dataType))
  }
}

private case class ArrayGetSet(dt : ArrayType) extends UDTypeGetSet[ArrayData] {
  private val componentGetSet : JDBCGetSet[_] = jdbcGetSet(dt.elementType)
  private val catalystConvert = UDTypeGetSet.ConvertToCatalyst(componentGetSet)
  override val sqlType: Int = Types.ARRAY

  def readDatum(datum : Datum) : ArrayData = {
    if (datum == null) {
      null
    } else {
      val oraArray = datum.asInstanceOf[oracle.jdbc.internal.OracleArray]
      val elemDatums = oraArray.getOracleArray
      val arr = scala.Array.fill[Any](elemDatums.size)(null)
      for ((elem, i) <- elemDatums.zipWithIndex) {
        if (elem != null) {
          arr(i) = catalystConvert.getCatalystVal(elem)
        }
      }
      new GenericArrayData(arr)
    }
  }

  override protected def readResultSet(rs: ResultSet, pos: Int): ArrayData = {
    readDatum(rs.getObject(pos).asInstanceOf[Datum])
  }

  override protected def setIRow(oRow: InternalRow, pos: Int, v: ArrayData): Unit = {
    oRow(pos) = v
  }

  override def readIRow(iRow: InternalRow, pos: Int): ArrayData = {
    iRow.get(pos, dt).asInstanceOf[ArrayData]
  }

  override protected def readLiteral(lit: Literal): ArrayData = {
    lit.value.asInstanceOf[ArrayData]
  }

  def constructJdbcObject(ps : PreparedStatement,
                          v : AnyRef,
                          oraDT : OraDataType
                         ): AnyRef = {
    val oraArrayDT = oraDT.asInstanceOf[OraArrayType]
    val catArr = v.asInstanceOf[ArrayData]

    val numElems = catArr.numElements()
    val oraObjArr = scala.Array.fill[AnyRef](numElems)(null)

    catArr.foreach(dt.elementType, {
      case (i, elemVal) =>
        if (catArr.isNullAt(i)) {
          oraObjArr(i) = null
        } else {
          val tVal = catalystConvert.getJdbcVal(elemVal)
          componentGetSet match {
            case cGS : UDTypeGetSet[_] =>
              oraObjArr(i) =
                cGS.constructJdbcObject(ps, tVal, oraArrayDT.elemType)
            case _ =>
              oraObjArr(i) = tVal.asInstanceOf[AnyRef]
          }
        }
    })

    ps.getConnection.asInstanceOf[OracleConnection].createARRAY(oraDT.oraTypeString, oraObjArr)
  }

  override protected def setPrepStat(ps: PreparedStatement,
                                     pos: Int,
                                     v: ArrayData,
                                     oraTableShape : scala.Array[OraColumn]): Unit = {
    ps.setObject(pos, constructJdbcObject(ps, v, oraTableShape(pos - 1).dataType))
  }
}

private case class MapGetSet(dt : MapType) extends UDTypeGetSet[MapData] {
  private val valueGetSet : JDBCGetSet[_] = jdbcGetSet(dt.valueType)
  private val catalystConvert = UDTypeGetSet.ConvertToCatalyst(valueGetSet)
  override val sqlType: Int = Types.ARRAY

  def readDatum(datum : Datum) : MapData = {
    if (datum == null) {
      null
    } else {
      val oraArray = datum.asInstanceOf[oracle.jdbc.internal.OracleArray]
      val elemDatums = oraArray.getOracleArray
      val keys = ArrayBuffer[Int]()
      val values = ArrayBuffer[Any]()
      for ((elem, i) <- elemDatums.zipWithIndex) {
        if ( elem != null) {
          keys += i
          values += catalystConvert.getCatalystVal(elem)
        }
      }
      new ArrayBasedMapData(new GenericArrayData(keys.toArray), new GenericArrayData(values))
    }
  }

  override protected def readResultSet(rs: ResultSet, pos: Int): MapData = {
    readDatum(rs.getObject(pos).asInstanceOf[Datum])
  }

  override protected def setIRow(oRow: InternalRow, pos: Int, v: MapData): Unit = {
    oRow(pos) = v
  }

  override def readIRow(iRow: InternalRow, pos: Int): MapData = {
    iRow.get(pos, dt).asInstanceOf[MapData]
  }

  override protected def readLiteral(lit: Literal): MapData = {
    lit.value.asInstanceOf[MapData]
  }

  def constructJdbcObject(ps : PreparedStatement,
                          v : AnyRef,
                          oraDT : OraDataType
                         ): AnyRef =
    throw new UnsupportedOperationException(s"No support for writing MapType")

  override protected def setPrepStat(ps: PreparedStatement,
                                     pos: Int,
                                     v: MapData,
                                     oraTableShape : scala.Array[OraColumn]): Unit = {
    ps.setObject(pos, constructJdbcObject(ps, v, oraTableShape(pos - 1).dataType))
  }
}

