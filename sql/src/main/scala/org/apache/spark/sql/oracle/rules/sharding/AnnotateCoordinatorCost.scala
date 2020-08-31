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

package org.apache.spark.sql.oracle.rules.sharding

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.oracle.OracleCatalog
import org.apache.spark.sql.connector.catalog.oracle.sharding.{CoordinatorQuery, ShardQueryInfo}
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.querysplit.OraExplainPlan


/**
 * for each [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation]]
 * that is of type [[CoordinatorQuery]], analyze its pushdown plan with `extractShardCosts=true`
 * and associate the [[PlanInfo]] with the [[ShardQueryInfo]]
 */
object AnnotateCoordinatorCost
  extends OraShardingLogicalRule
    with AnnotatePredicateHelper
    with Logging {

  override def _apply(plan: LogicalPlan)
                     (implicit sparkSession: SparkSession): LogicalPlan = {
    val oraCatalog = {
      sparkSession.sessionState.catalogManager.catalog("oracle").asInstanceOf[OracleCatalog]
    }
    val dsKey = oraCatalog.getMetadataManager.dsKey
    implicit val shardedMD = oraCatalog.getMetadataManager.getShardingMetadata

    logDebug(s"applying AnnotateCoordinatorCost on ${plan.treeString}")

    plan foreachUp  {
      case ds @ DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
        for (sInfo <- ShardQueryInfo.getShardingQueryInfo(ds)
            if (sInfo.queryType == CoordinatorQuery);
             planInfo <- OraExplainPlan.constructPlanInfo(dsKey, oraScan.oraPlan,
               false, true);
             newSInfo = sInfo.copy(planInfo = Some(planInfo))
             ) {
          ShardQueryInfo.setShardingQueryInfo(ds, newSInfo)
        }
      case _ => ()
    }

    plan
  }
}
