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

package org.apache.spark.sql.oracle.writepath

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import org.scalacheck.Gen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.testutils.DataGens.{not_null, null_percent_1, null_percent_15}
import org.apache.spark.sql.oracle.testutils.TestDataSetup._
import org.apache.spark.sql.types.StructType



trait WriteTestsDataSetup { self : AbstractWriteTests.type =>

  def constructRow(colValues : IndexedSeq[IndexedSeq[(_, Boolean)]], i : Int) : GenericRow = {
    new GenericRow(
      Array(
        colValues(0)(i)._1.asInstanceOf[String],
        colValues(1)(i)._1.asInstanceOf[String],
        colValues(2)(i)._1.asInstanceOf[String],
        colValues(3)(i)._1.asInstanceOf[String],
        colValues(4)(i)._1.asInstanceOf[String],
        colValues(5)(i)._1.asInstanceOf[String],
        colValues(6)(i)._1.asInstanceOf[String],
        colValues(7)(i)._1.asInstanceOf[String],
        colValues(8)(i)._1.asInstanceOf[BigDecimal].byteValueExact(),
        colValues(9)(i)._1.asInstanceOf[BigDecimal].shortValueExact(),
        colValues(10)(i)._1.asInstanceOf[BigDecimal].intValueExact(),
        colValues(11)(i)._1.asInstanceOf[BigDecimal].longValueExact(),
        colValues(12)(i)._1.asInstanceOf[BigDecimal],
        colValues(13)(i)._1.asInstanceOf[BigDecimal],
        colValues(14)(i)._1.asInstanceOf[BigDecimal],
        colValues(15)(i)._1.asInstanceOf[Date],
        colValues(16)(i)._1.asInstanceOf[Timestamp],
        colValues(17)(i)._1.asInstanceOf[String],
        colValues(18)(i)._1.asInstanceOf[String]
      )
    )
  }

  val unit_test_table_cols_ddl =
    """
      |C_CHAR_1         string      ,
      |C_CHAR_5         string      ,
      |C_VARCHAR2_10    string      ,
      |C_VARCHAR2_40    string      ,
      |C_NCHAR_1        string      ,
      |C_NCHAR_5        string      ,
      |C_NVARCHAR2_10   string      ,
      |C_NVARCHAR2_40   string      ,
      |C_BYTE           tinyint     ,
      |C_SHORT          smallint    ,
      |C_INT            int         ,
      |C_LONG           bigint      ,
      |C_NUMBER         decimal(25,0),
      |C_DECIMAL_SCALE_5 decimal(25,5),
      |C_DECIMAL_SCALE_8 decimal(25,8),
      |C_DATE           date        ,
      |C_TIMESTAMP      timestamp,
      |state            string,
      | channel          string""".stripMargin

  val unit_test_table_scheme: DataScheme = IndexedSeq[DataType[(_, Boolean)]](
    CharDataType(1).withNullsGen(not_null),
    CharDataType(5).withNullsGen(null_percent_1),
    Varchar2DataType(10).withNullsGen(null_percent_1),
    Varchar2DataType(40).withNullsGen(null_percent_15),
    NCharDataType(1).withNullsGen(null_percent_1),
    NCharDataType(5).withNullsGen(not_null),
    NVarchar2DataType(10).withNullsGen(null_percent_15),
    NVarchar2DataType(40).withNullsGen(null_percent_15),
    NumberDataType(2, 0).withNullsGen(not_null),
    NumberDataType(4, 0).withNullsGen(null_percent_1),
    NumberDataType(9, 0).withNullsGen(null_percent_15),
    NumberDataType(18, 0).withNullsGen(null_percent_15),
    NumberDataType(25, 0).withNullsGen(null_percent_15),
    NumberDataType(25, 5).withNullsGen(null_percent_15),
    NumberDataType(25, 8).withNullsGen(null_percent_15),
    // FloatDataType.withNullsGen(null_percent_15),
    // DoubleDataType.withNullsGen(null_percent_15),
    DateDataType.withNullsGen(null_percent_15),
    TimestampDataType.withNullsGen(null_percent_15),
    CharDataType(2).withGen(stateGen).withNullsGen(not_null),
    CharDataType(2).withGen(channelGen).withNullsGen(not_null),
  )

  val unit_test_table_catalyst_schema = StructType.fromDDL(unit_test_table_cols_ddl)

  val srcDataPath =
    "src/test/resources/data/src_tab_for_writes"

  def absoluteSrcDataPath : String =
    new java.io.File(srcDataPath).getAbsoluteFile.toPath.toString

  def write_src_df(numRows : Int) : Unit = {
    val seed = org.scalacheck.rng.Seed.random
    val params = Gen.Parameters.default.withInitialSeed(seed)

    val colValues : IndexedSeq[IndexedSeq[(_, Boolean)]] =
      unit_test_table_scheme.map(c =>
        Gen.listOfN(numRows, c.gen).apply(params, seed).get.toIndexedSeq
      )

    val rows : Seq[GenericRow] = (0 until numRows) map { i =>
      constructRow(colValues, i)
    }

    val rowsRDD : RDD[Row] = TestOracleHive.sparkContext.parallelize(rows, 1)

    TestOracleHive.
      createDataFrame(rowsRDD, unit_test_table_catalyst_schema).
      write.parquet(srcDataPath)
  }

}
