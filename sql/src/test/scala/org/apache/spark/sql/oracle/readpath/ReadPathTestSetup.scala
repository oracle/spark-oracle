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
package org.apache.spark.sql.oracle.readpath

import java.sql.Timestamp

import oracle.spark.{ConnectionManagement, DataSourceKey, ORAMetadataSQLs, ORASQLUtils}
import org.scalacheck.Gen

import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.testutils.{DataGens, TestDataSetup}

/**
 * - 'unit_test' and 'unit_test-partitioned' tables with
 *    many datatypes.
 * - not testing binary_float, binary_double because it is not supported by
 *   OracleWebRowSetXmlWriterImpl
 */
object ReadPathTestSetup {


  /*
   * Run with spark.sql.catalog.oracle.use_metadata_cache_only=false
   */
  def main(args : Array[String]) : Unit = {
    TestOracleHive.sql("use oracle")
    createAndLoad(ConnectionManagement.getDSKeyInTestEnv)
  }

  object SetupUser {
    val create_user =
      """
        |CREATE USER sparktest IDENTIFIED BY sparktest
        |""".stripMargin

    val grant_user =
      """
        |GRANT CONNECT, RESOURCE, unlimited tablespace TO sparktest
        |""".stripMargin
  }

  import DataGens._
  import TestDataSetup._

  val unit_test_table_ddl =
    """
      |create table unit_test(
      |  c_char_1 char(1),
      |  c_char_5 char(5),
      |  c_varchar2_10 varchar2(10),
      |  c_varchar2_40 varchar2(40),
      |  c_nchar_1 nchar(1),
      |  c_nchar_5 nchar(5),
      |  c_nvarchar2_10 nvarchar2(10),
      |  c_nvarchar2_40 nvarchar2(40),
      |  c_byte  number(2),
      |  c_short number(4),
      |  c_int number(9),
      |  c_long number(18),
      |  c_number number(25),
      |  c_decimal_scale_5 number(25,5),
      |  c_decimal_scale_8 number(25,8),
      |--  c_float binary_float,
      |--  c_double binary_double,
      |--  c_binary long,
      |  c_date date,
      |  c_timestamp timestamp
      |--  c_timestamp_with_tz timestamp with tz,
      |--  c_timestamp_with_local_tz timestamp with local tz
      |)
      |""".stripMargin

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
    TimestampDataType.withNullsGen(null_percent_15)
  )

  val unit_test_table_insert_stat =
    """
      |insert into unit_test
      | values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      | """.stripMargin

  val unit_test_partitioned_table_ddl =
    """
      |create table unit_test_partitioned(
      |  c_char_1 char(1),
      |  c_char_5 char(5),
      |  c_varchar2_10 varchar2(10),
      |  c_varchar2_40 varchar2(40),
      |  c_nchar_1 nchar(1),
      |  c_nchar_5 nchar(5),
      |  c_nvarchar2_10 nvarchar2(10),
      |  c_nvarchar2_40 nvarchar2(40),
      |  c_byte  number(2),
      |  c_short number(4),
      |  c_int number(9),
      |  c_long number(18),
      |  c_number number(25),
      |  c_decimal_scale_5 number(25,5),
      |  c_decimal_scale_8 number(25,8),
      |--  c_float binary_float,
      |--  c_double binary_double,
      |--  c_binary long,
      |  c_date date,
      |  c_timestamp timestamp,
      |--  c_timestamp_with_tz timestamp with tz,
      |--  c_timestamp_with_local_tz timestamp with local tz
      |  c_p_tstamp       timestamp(6) not null,
      |  c_p_deptno       number(2) not null
      |)
      |    PARTITION BY RANGE (c_p_tstamp)
      |    SUBPARTITION BY LIST (c_p_deptno)
      |(
      |    PARTITION p01 VALUES LESS THAN
      |        (TIMESTAMP '2010-01-01 00:00:00')
      |        (SUBPARTITION p01_sp1 VALUES (1),
      |                SUBPARTITION p01_sp2 VALUES (2),
      |                SUBPARTITION p01_sp3 VALUES (3),
      |                SUBPARTITION p01_sp4 VALUES (4)),
      |    PARTITION p02 VALUES LESS THAN
      |        (TIMESTAMP '2010-02-01 00:00:00')
      |        (SUBPARTITION p02_sp1 VALUES (1),
      |                SUBPARTITION p02_sp2 VALUES (2),
      |                SUBPARTITION p02_sp3 VALUES (3),
      |                SUBPARTITION p02_sp4 VALUES (4)),
      |    PARTITION p11 VALUES LESS THAN
      |        (TIMESTAMP '2010-11-01 00:00:00')
      |        (SUBPARTITION p11_sp1 VALUES (1,2),
      |                SUBPARTITION p11_sp2 VALUES (3,4)),
      |    PARTITION p12 VALUES LESS THAN
      |        (TIMESTAMP '2010-12-01 00:00:00'),
      |    PARTITION p13 VALUES LESS THAN
      |        (TIMESTAMP '2011-01-01 00:00:00')
      |)
      |""".stripMargin

  val unit_test_partitioned_table_scheme: DataScheme = unit_test_table_scheme ++
    IndexedSeq[DataType[(_, Boolean)]](
      TimestampDataType.withGen(
        Gen.oneOf(
          Gen.const(Timestamp.valueOf("2009-12-01 00:00:00")),
          Gen.const(Timestamp.valueOf("2010-01-01 00:00:00")),
          Gen.const(Timestamp.valueOf("2010-07-01 00:00:00"))
        )
      ).withNullsGen(not_null),
      NumberDataType(2, 0).withGen(
        Gen.oneOf((1 until 5).map(i => java.math.BigDecimal.valueOf(i))
        )
      ).withNullsGen(not_null)
    )

  val unit_test_partitioned_table_insert_stat =
    """
      |insert into unit_test_partitioned
      | values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      | """.stripMargin


  val unit_test_write_table_ddl =
    """
      |create table unit_test_write(
      |  c_char_1 char(1),
      |  c_char_5 char(5),
      |  c_varchar2_10 varchar2(10),
      |  c_varchar2_40 varchar2(40),
      |  c_nchar_1 nchar(1),
      |  c_nchar_5 nchar(5),
      |  c_nvarchar2_10 nvarchar2(10),
      |  c_nvarchar2_40 nvarchar2(40),
      |  c_byte  number(2),
      |  c_short number(4),
      |  c_int number(9),
      |  c_long number(18),
      |  c_number number(25),
      |  c_decimal_scale_5 number(25,5),
      |  c_decimal_scale_8 number(25,8),
      |--  c_float binary_float,
      |--  c_double binary_double,
      |--  c_binary long,
      |  c_date date,
      |  c_timestamp timestamp
      |--  c_timestamp_with_tz timestamp with tz,
      |--  c_timestamp_with_local_tz timestamp with local tz
      |)
      |""".stripMargin


  val unit_test_write_partitioned_table_ddl =
    """
      |create table unit_test_write_partitioned(
      |  c_varchar2_40 varchar2(40),
      |  c_int number(9),
        state            VARCHAR2(2),
      | channel          VARCHAR2(1)
      |)
      |PARTITION BY LIST (state, channel)
      |(
      |    PARTITION yearly_west_direct VALUES (('OR','D'),('UT','D'),('WA','D')),
      |    PARTITION yearly_west_indirect VALUES (('OR','I'),('UT','I'),('WA','I')),
      |    PARTITION yearly_south_direct VALUES (('AZ','D'),('TX','D'),('GA','D')),
      |    PARTITION yearly_south_indirect VALUES (('AZ','I'),('TX','I'),('GA','I')),
      |    PARTITION yearly_east_direct VALUES (('PA','D'), ('NC','D'), ('MA','D')),
      |    PARTITION yearly_east_indirect VALUES (('PA','I'), ('NC','I'), ('MA','I')),
      |    PARTITION yearly_north_direct VALUES (('MN','D'),('WI','D'),('MI','D')),
      |    PARTITION yearly_north_indirect VALUES (('MN','I'),('WI','I'),('MI','I')),
      |    PARTITION yearly_ny_direct VALUES ('NY','D'),
      |    PARTITION yearly_ny_indirect VALUES ('NY','I'),
      |    PARTITION yearly_ca_direct VALUES ('CA','D'),
      |    PARTITION yearly_ca_indirect VALUES ('CA','I'),
      |    PARTITION rest VALUES (DEFAULT)
      |    )
      |""".stripMargin

  def grantOnTable(tNm: String): String =
    s"grant all privileges on ${tNm} to public"

  def createAndLoad(dsKey: DataSourceKey): Unit = {

    val needsCreation = !ORAMetadataSQLs.tableExists(dsKey, None, "UNIT_TEST_PARTITIONED")

    if (needsCreation) {
      val r = TestDataSetup.createTable(dsKey, "unit_test", unit_test_table_ddl)
      ORASQLUtils.performDSDDL(dsKey, grantOnTable("unit_test"), s"grant access on 'unit_test'")
      TestDataSetup.loadTable(dsKey, "unit_test",
        unit_test_table_scheme,
        unit_test_table_insert_stat,
        1000
      )

      TestDataSetup.createTable(dsKey, "unit_test_partitioned", unit_test_partitioned_table_ddl)
      ORASQLUtils.performDSDDL(dsKey, grantOnTable("unit_test_partitioned"),
        s"grant access on 'unit_test_partitioned'")
      TestDataSetup.loadTable(dsKey, "unit_test_partitioned",
        unit_test_partitioned_table_scheme,
        unit_test_partitioned_table_insert_stat,
        1000
      )
    }
  }

}
