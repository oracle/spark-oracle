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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.oracle.{OracleCatalog, OraMetadataMgrInternalTest}
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.UnsupportedAction
import org.apache.spark.sql.hive.test.oracle.TestOracleHive

class TableCatalogAPITest extends AbstractTest with OraMetadataMgrInternalTest {

  test("describeTables") { td =>
    for ((ns, tbls) <- catalogTableMap if (loadOraSchemaForTests(ns));
         tbl <- tbls) {
      if (loadTableForTests(tbl)) {
        TestOracleHive.sql(s"describe extended ${ns}.${tbl}").show(1000, false)
      }
    }
  }

  test("createTable") { td =>
    /*
    1. Valid table creation, but no yet implemented
     */
    intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
           |create external table t2(id long, p string) 
           |using parquet 
           |partitioned by (p)
           |location "/tmp"""".stripMargin)
    }

    /*
     * 2. Valid table creation, but no yet implemented
     */
    // scalastyle:off line.size.limit
    var ex = intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
           |create table t2(id long, p string) 
           |using parquet 
           |partitioned by (p)
           |location "https://objectstorage.us-ashburn-1.oraclecloud.com/n/idlxex3qf8sf/b/SparkTest/o/t1"""".stripMargin)
    }

    if (OracleCatalog.oracleCatalog.catalogOptions.oci_credential_name.isDefined) {
      assert(
        ex.getMessage ==
          """Unsupported Action on Oracle Catalog: Cannot create table
            | Method is supported but hasn't been implemented yet""".stripMargin
      )
    } else {
      assert(
        ex.getMessage ==
          """Unsupported Action on Oracle Catalog: Cannot create table
            | Currently only object store resident tables of parquet format can be created
            | via Spark SQL. For other cases, create table using Oracle DDL""".stripMargin
      )
    }

    /*
     * 3. Valid table creation, but no yet implemented
     */

    ex = intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
                          |create table t2(id long, p string) 
                          |using parquet
                          |location "https://objectstorage.us-ashburn-1.oraclecloud.com/n/idlxex3qf8sf/b/SparkTest/o/t1"""".stripMargin)
    }
    // scalastyle:on

    if (OracleCatalog.oracleCatalog.catalogOptions.oci_credential_name.isDefined) {
      assert(
        ex.getMessage ==
          """Unsupported Action on Oracle Catalog: Cannot create table
            | Method is supported but hasn't been implemented yet""".stripMargin
      )
    } else {
      assert(
        ex.getMessage ==
          """Unsupported Action on Oracle Catalog: Cannot create table
            | Currently only object store resident tables of parquet format can be created
            | via Spark SQL. For other cases, create table using Oracle DDL""".stripMargin
      )
    }

    /*
     * 4. InValid table creation, not object store location
     */
    ex = intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
                            |create table t2(id long, p string) 
                            |using parquet
                            |location "/tmp"""".stripMargin)
    }

    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: Cannot create table
          | Currently only object store resident tables of parquet format can be created
          | via Spark SQL. For other cases, create table using Oracle DDL""".stripMargin)

  }

  test("dropTable") { td =>
    val ex = intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
                            |drop table tpcds.CALL_CENTER""".stripMargin)
    }

    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: drop table
          | For Oracle managed tables issue Oracle drop table DDL
          |For External tables: currently you have to issue Oracle drop table DDL""".stripMargin)

  }

  test("alterTable") { td =>
    val stats = Seq(
      s"""
       |alter table tpcds.store_sales
       |add columns
       | x int, y long""".stripMargin,
      s"""
       |alter table tpcds.store_sales
       |drop columns
       | ss_quantity""".stripMargin,
      s"""
         |alter table tpcds.store_sales
         |rename column
         |ss_quantity to ss_qty""".stripMargin,
      s"""
         |alter table tpcds.store_sales
         |SET TBLPROPERTIES
         | (a 'a')""".stripMargin,
      s"""
         |alter table tpcds.CALL_CENTER
         |change column
         | CC_TAX_PERCENTAGE type decimal(8,4)""".stripMargin)

    var ex: Exception = null

    for (stat <- stats) {
      ex = intercept[UnsupportedAction](TestOracleHive.sql(stat))
      assert(
        ex.getMessage ==
          """Unsupported Action on Oracle Catalog: alter table
            | For Oracle managed tables issue Oracle DDL
            |For External tables: Currently you have to drop and recreate table""".stripMargin)
    }
  }

  test("renameTable") { td =>
    val ex = intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
                            |alter table tpcds.CALL_CENTER
                            |rename to
                            | calling_center""".stripMargin)
    }

    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: rename table
          | For Oracle managed tables issue Oracle DDL
          |For External tables: Currently you have to drop and recreate table""".stripMargin)
  }

  val partTables = Seq(
    "SPARKTEST.COMP",
    "SPARKTEST.UT_SMALL_WRITE_MULTIROW_PARTITION",
    "SPARKTEST.UNIT_TEST_WRITE_PARTITIONED",
    "SPARKTEST.SALES_RANGE_PARTITION",
    "SPARKTEST.SALES_BY_REGION_UNKNOWN_VALUES",
    "SPARKTEST.SALES_BY_REGION ",
    "SPARKTEST.SALES_BY_REGION_AND_CHANNEL",
    "SPARKTEST.QUARTERLY_REGIONAL_SALES",
    "SPARKTEST.INTERVAL_PAR_DEMO",
    "SPARKTEST.HASH_PARTITION_TABLE",
    "SPARKTEST.UNIT_TEST_PARTITIONED",
    "TPCDS.CATALOG_RETURNS",
    "TPCDS.STORE_SALES")

  test("showPartitions") { td =>
    for (t <- partTables) {
      TestOracleHive.sql(
        s"show partitions $t"
      ).show(1000, false)
    }
  }

  test("showOraPartitions") { td =>
    for (t <- partTables) {
      TestOracleHive.sql(
        s"show oracle partitions $t"
      ).show(1000, false)
    }
  }

  test("qualShowOraPartitions") { td =>
    for (t <- partTables) {
      TestOracleHive.sql(
        s"show oracle partitions oracle.$t"
      ).show(1000, false)
    }
  }

  test("showOraPartitionsNegTests") { td =>

      val ex = intercept[AnalysisException] {
        TestOracleHive.sql(s"""show oracle partitions xyz""".stripMargin)
      }

      assert(
        ex.getMessage ==
          "Cannot run show oracle partitions command: failed to resolve table xyz"
      )

  }

}
