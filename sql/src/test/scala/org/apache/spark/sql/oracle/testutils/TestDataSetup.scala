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

package org.apache.spark.sql.oracle.testutils

import java.math.BigDecimal
import java.sql.{Date => SQLDate, PreparedStatement, Timestamp => SQLTimestamp, Types}

import oracle.spark.{DataSourceKey, ORASQLUtils}
import org.scalacheck.Gen

/**
 * Utility to `create` and `load` generated test data into
 * a table.
 */
object TestDataSetup {

  /**
   * represents a column datatype that:
   * - as an associated [[Gen]]
   * - encapsulates how a non-null value of the datatype is set
   *   in a [[PreparedStatement]]
   *
   * @tparam T
   */
  trait DataType[+T] {
    val sqlType : Int
    def setNonNullVal(ps : PreparedStatement,
                      col : Int,
                      v : Any) : Unit
    def gen : Gen[T]

    def withNullsGen(nullGen : Gen[Boolean]) : DataType[(T, Boolean)] =
      WithNulls(this, nullGen)

    def withGen[U >: T](gen : Gen[U]) : DataType[T] = WithGen(this, gen.asInstanceOf[Gen[T]])
  }

  case class WithNulls[T](underlying : DataType[T],
                          nullGen : Gen[Boolean]) extends DataType[(T, Boolean)] {
    override val sqlType: Int = underlying.sqlType

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit =
      underlying.setNonNullVal(ps, col, v)

    lazy val gen: Gen[(T, Boolean)] = DataGens.withNullFlag(underlying.gen, nullGen)
  }

  case class WithGen[T](underlying : DataType[T],
                      gen : Gen[T]) extends DataType[T] {
    override val sqlType: Int = underlying.sqlType

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit =
      underlying.setNonNullVal(ps, col, v)

  }

  case class CharDataType(length : Int) extends DataType[String] {
    override val sqlType: Int = Types.CHAR

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setString(col, v.asInstanceOf[String])
    }

    override def gen: Gen[String] = DataGens.sql_char(length)
  }

  case class Varchar2DataType(length : Int) extends DataType[String] {
    override val sqlType: Int = Types.VARCHAR

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setString(col, v.asInstanceOf[String])
    }

    override def gen: Gen[String] = DataGens.sql_varchar(length)
  }

  case class NCharDataType(length : Int) extends DataType[String] {
    override val sqlType: Int = Types.CHAR

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setString(col, v.asInstanceOf[String])
    }

    override def gen: Gen[String] = DataGens.sql_nchar(length)
  }

  case class NVarchar2DataType(length : Int) extends DataType[String] {
    override val sqlType: Int = Types.VARCHAR

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setString(col, v.asInstanceOf[String])
    }

    override def gen: Gen[String] = DataGens.sql_nvarchar(length)
  }

  case class NumberDataType(prec : Int, scale : Int) extends DataType[BigDecimal] {
    override val sqlType: Int = Types.VARCHAR

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setBigDecimal(col, v.asInstanceOf[BigDecimal])
    }

    override def gen: Gen[BigDecimal] = DataGens.sql_number(prec, scale)
  }

  case object FloatDataType extends DataType[Float] {
    override val sqlType: Int = Types.FLOAT

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setFloat(col, v.asInstanceOf[Float])
    }

    // from Oracle.Number
    override def gen: Gen[Float] = Gen.choose(-2.14748365E9F, 2.14748365E9F)
  }

  case object DoubleDataType extends DataType[Double] {
    override val sqlType: Int = Types.DOUBLE

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setDouble(col, v.asInstanceOf[Double])
    }

    // from Oracle.Number
    override def gen: Gen[Double] = Gen.choose(-2.147483648E9D, 2.147483647E9D)
  }

  case object DateDataType extends DataType[SQLDate] {
    override val sqlType: Int = Types.DATE

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setDate(col, v.asInstanceOf[SQLDate])
    }

    override def gen: Gen[SQLDate] = DataGens.sql_date
  }

  case object TimestampDataType extends DataType[SQLTimestamp] {
    override val sqlType: Int = Types.TIMESTAMP

    override def setNonNullVal(ps: PreparedStatement, col: Int, v: Any): Unit = {
      ps.setTimestamp(col, v.asInstanceOf[SQLTimestamp])
    }

    override def gen: Gen[SQLTimestamp] = DataGens.sql_timsetamp
  }


  type DataScheme = IndexedSeq[DataType[(_, Boolean)]]

  private trait DataRow {
    def scheme : DataScheme

    def get(col : Int) : Any
    def isNull(col : Int) : Boolean

    def setRow(ps : PreparedStatement) : Unit = {
      for ((dType, i) <- scheme.zipWithIndex) {
        if (isNull(i)) {
          ps.setNull(i + 1, dType.sqlType)
        } else {
          dType.setNonNullVal(ps, i + 1, get(i))
        }
      }
    }
  }

  private case class TableData(tblNm : String,
                       scheme : DataScheme,
                       insertStmt : String,
                       numRows : Int) { self =>

    val seed = org.scalacheck.rng.Seed.random
    val params = Gen.Parameters.default.withInitialSeed(seed)

    val colValues : IndexedSeq[List[(_, Boolean)]] =
      scheme.map(c => Gen.listOfN(numRows, c.gen).apply(params, seed).get)


    def populate(dsKey: DataSourceKey) : Unit = {

      var currentRow : Int = 0
      val dRow = new DataRow {
        override def scheme: DataScheme = self.scheme

        override def get(col: Int): Any = colValues(col)(currentRow)._1

        override def isNull(col: Int): Boolean = colValues(col)(currentRow)._2
      }

      val batchItr = new Iterator[PreparedStatement => Unit] {
        override def hasNext: Boolean = currentRow < numRows

        override def next(): PreparedStatement => Unit = {ps =>
          dRow.setRow(ps)
          currentRow += 1
        }
      }

      ORASQLUtils.performDSBatchDML(
        dsKey,
        insertStmt,
        s"load table ${tblNm}",
        ps => (),
        batchItr,
        1000
      )
    }

  }

  def createTable(dsKey : DataSourceKey,
                  tblNm : String,
                 createStat : String) : Int = {
    ORASQLUtils.performDSDDL(
      dsKey,
      createStat,
    s"Creating table ${tblNm}")
  }

  def loadTable(dsKey : DataSourceKey,
                tblNm : String,
                scheme : DataScheme,
                insertStmt : String,
                numRows : Int) : Unit = {
    val tblData = TableData(tblNm, scheme, insertStmt, numRows)
    tblData.populate(dsKey)
  }


}
