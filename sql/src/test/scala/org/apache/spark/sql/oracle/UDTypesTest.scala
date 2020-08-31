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

import oracle.spark.{ConnectionManagement, ORASQLUtils}

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.oracle.OracleCatalog
import org.apache.spark.sql.hive.test.oracle.TestOracleHive

class UDTypesTest extends AbstractTest with PlanTestHelpers {

  val test_table = "unit_test_table_udts"

  val types_ddl = Seq(
    """CREATE OR REPLACE TYPE sparktest.unit_test_rec_type_1 AS OBJECT
          ( Street       VARCHAR2(80),
              City         VARCHAR2(80),
              State        CHAR(2),
              Zip          VARCHAR2(10)
          )""",
    """CREATE OR REPLACE TYPE sparktest.unit_test_rec_type_2 AS OBJECT
        ( address       sparktest.unit_test_rec_type_1,
          name          VARCHAR2(80)
        )""",
    """create or replace type sparktest.unit_test_table_type_1
      | as table of sparktest.unit_test_rec_type_1""".stripMargin,
    """create or replace type sparktest.unit_test_table_type_2
      |  as varray(100) of sparktest.unit_test_rec_type_1""".stripMargin,
    """create or replace type sparktest.unit_test_table_type_3 as table of varchar2(200)"""
  )

  val table_ddl = s"""create table sparktest.${test_table}(
                    |    col1 sparktest.unit_test_rec_type_1,
                    |    col2 sparktest.unit_test_rec_type_2,
                    |    col3 sparktest.unit_test_table_type_1,
                    |    col4 sparktest.unit_test_table_type_2
                    |) NESTED TABLE col3 STORE AS ${test_table}_col3""".stripMargin

  val table_insert =
   s"""INSERT ALL
     |    INTO sparktest.${test_table}
     |      VALUES (sparktest.unit_test_rec_type_1('a', 'San Jose', 'CA', '11111'),
     |              sparktest.unit_test_rec_type_2(sparktest.unit_test_rec_type_1('a', 'New York', 'NY', '21111'), 'first'),
     |              sparktest.unit_test_table_type_1(
     |                      sparktest.unit_test_rec_type_1('a', 'San Jose', 'CA', '11111'),
     |                      sparktest.unit_test_rec_type_1('a', 'San Jose', 'CA', '11111')
     |                  ),
     |              sparktest.unit_test_table_type_2(
     |                      sparktest.unit_test_rec_type_1('a', 'San Jose', 'CA', '11111'),
     |                      sparktest.unit_test_rec_type_1('a', 'San Jose', 'CA', '11111')
     |                  )
     |             )
     |    INTO sparktest.${test_table}
     |        VALUES (sparktest.unit_test_rec_type_1('b', 'San Jose', 'CA', '11111'),
     |                sparktest.unit_test_rec_type_2(sparktest.unit_test_rec_type_1('b', 'New York', 'NY', '21111'), 'second'),
     |                sparktest.unit_test_table_type_1(
     |                        sparktest.unit_test_rec_type_1('b', 'San Jose', 'CA', '11111'),
     |                        sparktest.unit_test_rec_type_1('b', 'San Jose', 'CA', '11111')
     |                    ),
     |                sparktest.unit_test_table_type_2(
     |                        sparktest.unit_test_rec_type_1('b', 'San Jose', 'CA', '11111'),
     |                        sparktest.unit_test_rec_type_1('b', 'San Jose', 'CA', '11111')
     |                    )
     |               )
     |    INTO sparktest.${test_table}
     |        VALUES (sparktest.unit_test_rec_type_1('c', 'San Jose', 'CA', '11111'),
     |                sparktest.unit_test_rec_type_2(sparktest.unit_test_rec_type_1('c', 'New York', 'NY', '21111'), 'third'),
     |                sparktest.unit_test_table_type_1(
     |                        sparktest.unit_test_rec_type_1('c', 'San Jose', 'CA', '11111'),
     |                        sparktest.unit_test_rec_type_1('c', 'San Jose', 'CA', '11111')
     |                    ),
     |                sparktest.unit_test_table_type_2(
     |                        sparktest.unit_test_rec_type_1('c', 'San Jose', 'CA', '11111'),
     |                        sparktest.unit_test_rec_type_1('c', 'San Jose', 'CA', '11111')
     |                    )
     |               )
     |SELECT 1 FROM DUAL""".stripMargin

  val function_ddls = Seq(
  """CREATE OR REPLACE FUNCTION sparktest.unit_test_fn_1(x sparktest.unit_test_rec_type_1)
    |    RETURN VARCHAR IS
    |BEGIN
    |    RETURN x.State;
    |END;
    |/""".stripMargin,
    """CREATE OR REPLACE FUNCTION sparktest.unit_test_fn_2(x sparktest.unit_test_table_type_1)
      |    RETURN sparktest.unit_test_rec_type_1 IS
      |BEGIN
      |    RETURN x(1);
      |END;
      |/""".stripMargin
  )

  val star_query = s"select * from sparktest.${test_table}"
  val fn1_query = s"select oracle.unit_test_fn_1(col1) from sparktest.${test_table}"
  val fn2_query = s"select oracle.unit_test_fn_2(col3) from sparktest.${test_table}"

  val insert =
    s"""insert into sparktest.${test_table} values
       |(('b', 'San Jose', 'CA', '11111'),
       | (('b', 'San Jose', 'CA', '11111'), 'fourth'),
       | null,
       | array(
       |   ('c', 'San Jose', 'CA', '11111'),
       |    ('c', 'San Jose', 'CA', '11111')
       | ))""".stripMargin

  val delete =
    s"""delete from sparktest.${test_table}
       |where col3 is null""".stripMargin

  /*
   * Call to create the types and table in Oracle instance
   * During normal test running we keep the metadata in checked in cache, so tests run faster.
   */
  private def checkAndSetup : Unit = {

    def setupTypesAndTables: Unit = {
      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      // try but don't throw because Types might already exist.
      scala.util.Try {
        types_ddl.map(ORASQLUtils.performDSDDL(dsKey, _, "Setup for UDTypesTest"))
        Seq(
          "unit_test_rec_type_1",
          "unit_test_rec_type_2",
          "unit_test_table_type_1",
          "unit_test_table_type_2"
        ).foreach(nm =>
          ORASQLUtils.performDSDDL(
            dsKey,
            s"grant all privileges on sparktest.$nm to public",
            "Setup for UDTypesTest")
        )
      }

      ORASQLUtils.performDSDDL(dsKey, table_ddl, "Setup for UDTypesTest")
      ORASQLUtils.performDSDML(dsKey, table_insert, "Setup for UDTypesTest")
      function_ddls.map(ORASQLUtils.performDSDDL(dsKey, _, "Setup for UDTypesTest"))
    }

    try {
      OracleCatalog.oracleCatalog.
        loadTable(Identifier.of(Array("SPARKTEST"), test_table))
    } catch {
      case ex : NoSuchTableException => setupTypesAndTables
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // checkAndSetup
    TestOracleHive.sparkSession.registerOracleFunctions(None, "UNIT_TEST_FN_1")
    TestOracleHive.sparkSession.registerOracleFunctions(None, "UNIT_TEST_FN_2")
  }

  test("loadTypes") {td =>
    TestOracleHive.sparkSession.registerOraType("SPARKTEST", "UNIT_TEST_TABLE_TYPE_1")
  }

  test("describe") {td =>
    TestOracleHive.sql(s"describe sparktest.${test_table}").show(1000, false)
  }

  test("explain_select") { td =>
    for (q <- Seq(star_query, fn1_query, fn2_query)) {
      TestOracleHive.sql(s"explain oracle pushdown ${q}").
        show(1000, false)
    }
  }

  test("select") {td =>
    TestOracleHive.sql(star_query).show(1000, false)
    TestOracleHive.sql(fn1_query).show(1000, false)

    // Have to do a collect because don't have pushdown for cast of a struct.
    // See note in OraPushdown about partial pushdown
    val r = TestOracleHive.sql(fn2_query).collect()
    // scalastyle:off println
    println(r.mkString("\n"))
  }

  test("insert") {td =>
    try {
      TestOracleHive.sql(insert).show(1000, false)
    } finally {
       TestOracleHive.sql(delete).show(1000, false)
    }
  }

}
