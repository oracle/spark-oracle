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

import org.apache.spark.sql.{Dataset, SPARK_LEGACY_INT96}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.oracle.OraPushdownScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.{AbstractTest, PlanTestHelpers}
import org.apache.spark.sql.oracle.querysplit.OraExplainPlan
import org.apache.spark.sql.oracle.rules.OraUndoSQLPushdown
import org.apache.spark.sql.oracle.tpcds.TPCDSQueries.TPCDSQuerySpec

/**
 * Test [[OraUndoSQLPushdown]] on all TPCDS queries.
 * For each query recursively Undo the Plan until it contains no [[OraPushdownScan]].
 * For each query there are 10-100s of undo steps. For each step
 * we valid the oracle psuhdown sql by running an explain.
 * So this is a very long running test(around 23 minutes).
 *
 * We are consistently seeing failure for 3 queries: 14-1,33, 56.
 * This doesn't seem related to undo functionality.
 * Fails with SQLRecoverableException, IO operation timeout.
 * When issuing the explain statement that causes the failure, you can observe the
 * client is waiting for several seconds.
 * TODO: analyze failure on Oracle server.
 */
class TracePushDownTest extends AbstractTest with PlanTestHelpers {

  // scalastyle:off println
  def printPlanAtStep(plan: LogicalPlan, stepNum: Int): Unit = {
    println(s"""Plan at Step ${stepNum}:
         |${plan.treeString}
         |""".stripMargin)
  }

  def validatePlan(plan: LogicalPlan, stepNum: Int): Unit = {
    val p = Dataset.ofRows(TestOracleHive.sparkSession, plan)
    p.queryExecution.sparkPlan
  }

  def runOraExplain(plan: LogicalPlan, stepNum: Int): Unit = {
    val pushdownOraScan = plan.collectFirst {
      case dsv2 @ DataSourceV2ScanRelation(_, oScan: OraPushdownScan, _) => oScan
    }
    if (pushdownOraScan.isDefined) {
      if (false) {
        println(pushdownOraScan.get.oraPlan.treeString)
        println(pushdownOraScan.get.oraPlan.orasql.sql)
      }
      OraExplainPlan.constructPlanInfo(
        pushdownOraScan.get.dsKey,
        pushdownOraScan.get.oraPlan,
        false)
    }
  }

  def traceQuery(qNm: String, q: TPCDSQuerySpec): Unit = {
    println(s"Query ${qNm}:")
    val plan = TestOracleHive.sql(q.sql).queryExecution.optimizedPlan

    OraUndoSQLPushdown.traceOraPushdown(plan) { (plan: LogicalPlan, stepNum: Int) =>
      if (false) {
        printPlanAtStep(plan, stepNum)
      } else {
        println(s"Query ${qNm}, step ${stepNum}")
      }
      if (false) {
        validatePlan(plan, stepNum)
      }
      runOraExplain(plan, stepNum)
    }
    println("-------------------------------------------------------------")
  }
  // scalastyle:on

  ignore("traceQueries") { _ =>
    val failedQueries = Set("q56", "q33", "q14-1")
    for ((qNm, q) <- TPCDSQueries.queries if !failedQueries.contains(qNm)) {
      traceQuery(qNm, q)
    }
  }

}
