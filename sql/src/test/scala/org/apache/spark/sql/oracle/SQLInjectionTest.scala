/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.oracle

import scala.util.Try

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests that check for SQL Injection attacks. Based on examples
 * [[https://vulncat.fortify.com/en/detail?id=desc.dataflow.abap.sql_injection here]] and
 * internal Oracle documentation.
 *
 * These tests require the test instance to have the tables defined in
 * `test/resources/sqlInjectDDLs.sql`. These tests run only if these tables are present.
 * Currently you need to run `sqlInjectDDLs.sql` manually on your test instance to enable these
 * tests.
 */
class SQLInjectionTest extends AbstractTest with PlanTestHelpers {

  import SQLInjectionTest._

  private var runTest: Boolean = false

  override def beforeAll(): Unit = {
    TestOracleHive.setConf(SQLConf.CASE_SENSITIVE, true)
    TestOracleHive.sql("use oracle")
    runTest = Try { TestOracleHive.sql("describe `schemaa`.`a`").collect() }.isSuccess
  }

  override def afterAll(): Unit = {
    TestOracleHive.setConf(SQLConf.CASE_SENSITIVE, false)
    super.afterAll()
  }

  private def testSQLInjection(nm: String, query: SQLInjectQuery): Unit = {
    test(nm) { _ =>
      if (runTest) {
        val rows = TestOracleHive.sql(query.sql).collect()

        assert(rows.size == query.result.size)

        for ((row, eRow) <- rows.zip(query.result)) {
          for (i <- 0 until row.size) {
            assert(
              (row.isNullAt(i) && eRow.isNullAt(i)) ||
                row.getString(i) == eRow.getString(i))
          }
        }
      }
    }
  }

  testSQLInjection("mixedCase", mixedCase)
  testSQLInjection("commentInValue", commentInValue)
  testSQLInjection("eqExprInValue", eqExprInValue)
  testSQLInjection("commentInColName", commentInColName)
  testSQLInjection("quoteInValue", quoteInValue)
  testSQLInjection("semiColonStatInValue", semiColonStatInValue)

}

object SQLInjectionTest {

  val ROW_1 = Row("A_a_a", "B_a_a")
  val ROW_2 = Row("'A_a_a' or 1==1", null)

  case class SQLInjectQuery(sql: String, result: Seq[Row])

  val mixedCase = SQLInjectQuery(
    """select `a`, `A`
      |from `schemaa`.`a`""".stripMargin,
    Seq(ROW_1, ROW_2))

  val commentInValue = SQLInjectQuery(
    """select *
      |from `schemaa`.`a`
      |where `a` = "fred --" or
      |      `A` = "B_a_a"""".stripMargin,
    Seq(Row("A_a_a", "B_a_a", "C_a_a", "D_a_a")))

  val eqExprInValue = SQLInjectQuery(
    """select `a`
      |from `schemaa`.`a`
      |where `a` = "'A_a_a' or 1==1"
      |      and `A` is not null""".stripMargin,
    Seq.empty[Row])

  val commentInColName = SQLInjectQuery(
    """
      |select `a`, `A`
      |from `schemaa`.`a`
      |where `a` is null or
      |      `--a` = 'C_a_a'""".stripMargin,
    Seq(ROW_1))

  val quoteInValue = SQLInjectQuery(
    """
      |select `a`
      |from `schemaa`.`a`
      |where `a` = "name' OR 'a'='a"""".stripMargin,
    Seq.empty[Row])

  val semiColonStatInValue = SQLInjectQuery(
    """select `a`
      |from `schemaa`.`a`
      |where `a` = "'name';\nDELETE FROM items;\n--'"""".stripMargin,
    Seq.empty[Row])

}
