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
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.connector.read.oracle.{OraPushdownScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.OraSparkConfig
import org.apache.spark.sql.oracle.rules.OraUndoSQLPushdown

/**
 * For [[CoordinatorQuery CoordinateQueries]] if the query is deemed 'expensive' by
 * 'COST' or 'OPERATOR' modes than it is replaced by the [[LogicalPlan a Spark Plan]] that it was
 * constructed from. The [[OraUndoSQLPushdown undo pushdown action]] is recrusively applied to plans
 * until the Plan contains no [[CoordinatorQuery CoordinateQueries]].
 *
 * So once we identify a [[CoordinatorQuery]] we recursively undo pushdown until the plan doesn't
 * contain any [[CoordinatorQuery CoordinateQueries]]. An intermediate plan in this undo
 * sequence may be a different [[CoordinatorQuery]] Scan; we could check
 * whether the new plan is a costly [[CoordinatorQuery]] before undoing it;
 * but we don't and continue to undo until all [[OraScan oracle queries]] are [[ShardedQuery]]
 * or [[ReplicatedQuery]]; this because the check whether a [[CoordinatorQuery]] is costly
 * requires running an expensive `explain <generated oracle query>` on the oracle instance.
 *
 * In most cases this should be ok; for plans where
 * the coordinator is not doing a lot of work, if we replace this by a Plan that does coordination
 * via Spark operators, the overall Plan cost should not increase by a lot.
 */
object RewriteAsShardPlan extends OraShardingLogicalRule
  with AnnotatePredicateHelper
  with Logging {

  def isPushdownCandidate(dsv2 : DataSourceV2ScanRelation)
                         (implicit sparkSession: SparkSession) : Boolean = {

    val sInfo = ShardQueryInfo.getShardingQueryInfo(dsv2)
    val planInfo = sInfo.flatMap(_.planInfo)
    val shardPlanInfoO = planInfo.flatMap(_.shardingPlanInfo)

    if (shardPlanInfoO.isDefined) {
      val shardPlanInfo = shardPlanInfoO.get
      val pushdown_mode =
        OraSparkConfig.getConf(OraSparkConfig.SHARDING_PUSHDOWN_MODE)

      if ( pushdown_mode == "COST") {
        val avgShardPlanTime : Double = (shardPlanInfo.shardWorkersTime.toDouble /
          shardPlanInfo.numShardWorkersQueries)
        (shardPlanInfo.totalTime.toDouble - avgShardPlanTime)  >
          OraSparkConfig.getConf(OraSparkConfig.SHARDING_PUSHDOWN_COST_THRESHOLD).toDouble
      } else {
        shardPlanInfo.hasTableAccess || shardPlanInfo.hasJoins ||
          shardPlanInfo.numShardWorkersQueries > 1
      }
    } else {
      false
    }
  }

  def shardPushdown(plan : LogicalPlan) : Option[LogicalPlan] = {

    def doPushdown(dsv2  : DataSourceV2ScanRelation,
                   oScan : OraScan) : Boolean = {
      val sInfo = ShardQueryInfo.getShardingQueryInfo(dsv2)

      oScan.oraPlan.catalystOp.isDefined &&
        sInfo.isDefined &&
        sInfo.get.queryType == CoordinatorQuery
    }


    val oraPushDSv2 = plan.collectFirst {
      case dsv2@DataSourceV2ScanRelation(_, oScan : OraScan, _)
        if doPushdown(dsv2, oScan) => dsv2
    }

    oraPushDSv2.map { undoOp =>
      plan.transformUp {
        case op if op == undoOp =>
          undoOp.scan.asInstanceOf[OraPushdownScan].oraPlan.catalystOp.get
      }
    }
  }

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan = {
    val oraCatalog = {
      sparkSession.sessionState.catalogManager.catalog("oracle").asInstanceOf[OracleCatalog]
    }
    implicit val shardedMD = oraCatalog.getMetadataManager.getShardingMetadata

    logDebug(s"applying RewriteAsShardPlan on ${plan.treeString}")

    val r = plan transformUp  {
      case ds @ DataSourceV2ScanRelation(_, _, _) if isPushdownCandidate(ds) =>
        OraUndoSQLPushdown.undoCoordinatorQueries(ds)(shardPushdown _)
    }

    r
  }
}
