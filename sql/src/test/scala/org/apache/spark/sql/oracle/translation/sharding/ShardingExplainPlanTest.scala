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

package org.apache.spark.sql.oracle.translation.sharding

import org.apache.spark.sql.connector.catalog.oracle.sharding.{CoordinatorQuery, ShardQueryInfo}
import org.apache.spark.sql.connector.read.oracle.OraPushdownScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.querysplit.OraExplainPlan
import org.apache.spark.sql.oracle.tpch.TPCHQueries

class ShardingExplainPlanTest extends AbstractShardingTranslationTest {

  // scalastyle:off println
  ignore("annotateCoordCost") {_ =>
    for ((qNm, q) <- TPCHQueries.queries) {
      val plan = TestOracleHive.sql(s"$q").queryExecution.optimizedPlan

      val dsvO = plan.collectFirst {
        case dsv2 @ DataSourceV2ScanRelation(_, oScan: OraPushdownScan, _) => dsv2
      }

      for (dsv2 <- dsvO;
           sInfo <- ShardQueryInfo.getShardingQueryInfo(dsv2) if sInfo.queryType == CoordinatorQuery
      ) {
        assert(sInfo.planInfo.isDefined)
        println(s"Query ${qNm}:")
        sInfo.planInfo.get.explain(s => System.out.print(s))
      }

      println("-------------------------------------------------------------")
    }
  }

  ignore("annotateShardingInfo") {_ =>
    for ((qNm, q) <- TPCHQueries.queries) {
      println(s"Query ${qNm}:")
      val plan = TestOracleHive.sql(s"$q").queryExecution.optimizedPlan

      val pushdownOraScan = plan.collectFirst {
        case dsv2 @ DataSourceV2ScanRelation(_, oScan: OraPushdownScan, _) => oScan
      }

      if (pushdownOraScan.isDefined) {
        // println(OraExplainPlan.oraExplainPlanXML(dsKey, pushdownOraScan.get.oraPlan))
        OraExplainPlan.constructPlanInfo(dsKey,
          pushdownOraScan.get.oraPlan,
          false, true
        ).get.explain(s => System.out.print(s))

      }

      println("-------------------------------------------------------------")
    }
  }
  // scalastyle:on println

}
