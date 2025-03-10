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

import org.apache.spark.sql.hive.test.oracle.TestOracleHive

class FunctionRegistrationTest extends AbstractTest
  with PlanTestHelpers {

  val standard_funcs : Seq[AnyRef] = Seq(
    "ADD_MONTHS", "BITAND", "LAST_DAY",
    "MONTHS_BETWEEN", "NEXT_DAY", "REGEXP_COUNT", "REGEXP_INSTR",
    "REGEXP_REPLACE", "REGEXP_SUBSTR",
    "USER", ("SYS_CONTEXT", "ora_context")
    /* , "XOR"  we don't support pl_sql boolean type */
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    import org.apache.spark.sql.oracle._

    // scalastyle:off println
    println(
      TestOracleHive.sparkSession.registerOracleFunctions(Some("STANDARD"), standard_funcs : _*)
    )

    println(
      TestOracleHive.sparkSession.registerOracleFunctions(Some("DBMS_STANDARD"), "LOGIN_USER")
    )

    println(
      TestOracleHive.sparkSession.registerOracleFunctions(Some("DBMS_UTILITY"), "GET_CPU_TIME")
    )

    println(
      TestOracleHive.sparkSession.registerOracleFunctions(None, "STRAGG")
    )

    // scalastyle:on
  }

  val q1 = """
             |select oracle.ADD_MONTHS(C_DATE, 1) add_months,
             |       oracle.bitand(c_short, c_int) bit_and,
             |       oracle.LAST_DAY(C_DATE) last_day,
             |       oracle.MONTHS_BETWEEN(oracle.NEXT_DAY(C_DATE, 'TUESDAY'),
             |       oracle.LAST_DAY(C_DATE)) mon_betw,
             |       oracle.user() ouser,
             |       oracle.ORA_CONTEXT('USERENV', 'CLIENT_PROGRAM_NAME') ora_client_pgm,
             |       oracle.login_user() login_usr,
             |       oracle.GET_CPU_TIME() cpu_time
             |from sparktest.unit_test
             |""".stripMargin

  val q2 =
    """
      |select c_char_5, oracle.stragg(c_char_1)
      |from sparktest.unit_test
      |group by c_char_5""".stripMargin


  test("register-and_query") { td =>
    TestOracleHive.sql(q1).show()
  }

  test("pushdown-off") { td =>

    try {
      OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, false)
      val ex: Exception = intercept[UnsupportedOperationException] {
        TestOracleHive.sql(q1).show()
      }
      println(ex.getMessage)
    } finally {
      OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, true)
    }
  }

  test("aggfunc") {td =>
    TestOracleHive.sql(q2).show()
  }

}
