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

package org.apache.spark.sql.oracle.tpcds

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.hive.test.oracle.{OracleTestConf, TestOracleHive}
import org.apache.spark.sql.oracle.{AbstractTest, PlanTestHelpers}
import org.apache.spark.sql.oracle.tpcds.TPCDSQueries.TPCDSQuerySpec

// scalastyle:off println
class CheckScansTest extends AbstractTest with PlanTestHelpers {

  def performTest(qNm: String, q: TPCDSQuerySpec): Unit = {
    test(qNm) { _ =>
      /*
      TODO: recreate ScanDetails

      validateOraScans(q.sql, q.scanDetailsMap)
      TestOracleHive.sql(s"explain formatted ${q.sql}").show(false)

       */

      if (OracleTestConf.runLongTest()) {
        showOraQueries(qNm, q.sql)
      } else {
        println(s"Skipping test ${qNm} based on 'spark.oracle.test.long_test.run.prob'")
      }

    }
  }

  /*
   * Use this test to execute a query
   */
  ignore("try_q") {td =>
    // OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, false)

    showOraQueries("q66", TPCDSQueries.QuerySet1.q66.sql)
    val df = TestOracleHive.sql(TPCDSQueries.QuerySet1.q66.sql)
      System.out.println(s"plan is:")
      System.out.println(df.queryExecution.optimizedPlan.treeString)
      df.show(1000, false)
    }

  /*
   * Use this test to execute all queries
   * and get a list of failures.
   */
  ignore("exec") { td =>

    // Failed queries q66

      /*
       * enable querySplitting
       * set task target to 4MB
       * max fetch tasks = 4 on a laptop with 8 task-slots(cores)
       */
    TestOracleHive.setConf("spark.sql.oracle.enable.querysplitting", "true")
    TestOracleHive.setConf("spark.sql.oracle.querysplit.target", "4MB")
    TestOracleHive.setConf("spark.sql.oracle.querysplit.maxfetch.rounds", "0.5")

    val ab = ArrayBuffer[String]()

    def include(qNm : String) : Boolean = {
      // Set("q24-2", "q80", "q47").contains(qNm)
      true
    }

    val excludeSet = Set("q14-1", "q14-2", "q23-2")

    val resList = ArrayBuffer[(String, Long)]()

    for ((qNm, q) <- TPCDSQueries.queries if include(qNm) && !excludeSet.contains(qNm)) {
      try {
        println(s"Query ${qNm} : ")
        val df = TestOracleHive.sql(q.sql)
        val sTime = System.currentTimeMillis()
        df.show(10000000, false)
        val eTime = System.currentTimeMillis()
        resList += ((qNm, (eTime-sTime)))
      } catch {
        case t : Throwable =>
        println(s"executing query ${qNm} FAILED : ")
        t.printStackTrace()
        ab += qNm
      }
    }

    resList.sortBy(t => t._2).foreach {t => println(s"${t._1} = ${t._2 / 1000.0} secs")}
    println(s"Failed queries ${ab.mkString(",")}")
  }

  /*
  * Use this test to list queries that
  * whose pushdown plan has only some
  * post-processing done in Spark.
  */
  ignore("pushdownList") { td =>
  val sb = new StringBuilder
    for ((qNm, q) <- TPCDSQueries.queries) {
      val plan = TestOracleHive.sql(q.sql).queryExecution.optimizedPlan
      val oraQueries = collectOraQueryBlocks(plan)
      if (oraQueries.size == 1) {
        sb.append(
          s"""
             |Query ${qNm}:
             |${plan.treeString}
             |oracle sql:
             |${oraQueries.head.orasql.sql}
             |""".stripMargin
        )
      }
    }
    println(sb)
  }

  /*
 * Use this test to list queries that
 * whose pushdown plan has significant
 * processing done in Spark.
 */
  ignore("nonPushdownList") { td =>
    val sb = new StringBuilder
    for ((qNm, q) <- TPCDSQueries.queries) {
      val plan = TestOracleHive.sql(q.sql).queryExecution.optimizedPlan
      val oraQueries = collectOraQueryBlocks(plan)
      if (oraQueries.size > 1) {
        sb.append(
          s"""
             |Query ${qNm}:
             |${plan.treeString}
             |oracle sqls:
             |${oraQueries.map(_.orasql.sql).mkString("\n\n")}
             |""".stripMargin
        )
      }
    }
    println(sb)
  }

  for ((qNm, q) <- TPCDSQueries.queries) {
    performTest(qNm, q)
  }

}
