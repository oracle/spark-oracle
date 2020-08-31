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
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftAnti}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.oracle.OracleCatalog
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.connector.catalog.oracle.sharding.ShardQueryInfo._
import org.apache.spark.sql.connector.read.oracle.{OraFileScan, OraPushdownScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.operators.OraTableScan

object AnnotateShardingInfoRule
    extends OraShardingLogicalRule
    with AnnotatePredicateHelper
    with Logging
    with FilterAnnotate
    with JoinAnnotate
    with AggregateAnnotate {

  private[sql] def project(from: LogicalPlan, proj: Project)(
      implicit sparkSession: SparkSession,
      shardedMD: ShardingMetadata): Unit = {
    val sInfo = getShardingQueryInfoOrCoord(from)
    setShardingQueryInfo(proj, sInfo)
  }

  private def annotate(plan: LogicalPlan)(
      implicit sparkSession: SparkSession,
      shardedMD: ShardingMetadata): LogicalPlan = {
    plan foreachUp {
      case plan if ShardQueryInfo.hasShardingQueryInfo(plan) => ()
      case ds @ DataSourceV2ScanRelation(_, oraFScan: OraFileScan, _) =>
        val oraTabScan : OraTableScan = oraFScan.oraPlan
        val oraTab = oraTabScan.oraTable
        val shardQInfo = shardedMD.shardQueryInfo(oraTab)
        logInfo(s"annotate DSV2 for ${oraTab.name} with ShardQueryInfo ${shardQInfo.show}")
        ShardQueryInfo.setShardingQueryInfo(ds, shardQInfo)
        // take into account, filters associated with the OraTableScan
        filter(oraTabScan, ds)
      case ds @ DataSourceV2ScanRelation(_, oraScan: OraPushdownScan, _) =>
        val oraPlan = oraScan.oraPlan
        val catalystOp = oraPlan.catalystOp
        if (catalystOp.isDefined) {
          val catOp = catalystOp.get
          annotate(catOp)
          ShardQueryInfo.getShardingQueryInfo(catOp).map { sQI =>
            ShardQueryInfo.setShardingQueryInfo(ds, sQI)
          }
        } else {
          ShardQueryInfo.setShardingQueryInfo(ds, shardedMD.COORD_QUERY_INFO)
        }
      case p @ Project(_, child) =>
        project(child, p)
      case f @ Filter(_, child) =>
        filter(child, f)
      case joinOp @ ExtractEquiJoinKeys(
            joinType,
            leftKeys,
            rightKeys,
            joinCond,
            _,
            _,
            _)  if joinType != ExistenceJoin =>
        if (joinType == LeftAnti) {
          nonInJoin(joinOp, leftKeys, rightKeys, joinCond)
        } else {
          equiJoin(joinOp, leftKeys, rightKeys)
        }
      case joinOp@Join(_, _, joinType, joinCond, _) =>
        if (joinType == LeftAnti) {
          nonInJoin(joinOp, Seq.empty, Seq.empty, joinCond)
        } else {
          nonEquiJoin(joinOp)
        }
      case aggOp @ Aggregate(_, _, child) =>
        aggregate(child, aggOp)
      case op =>
        val childSInfos =
          op.children.map(plan => ShardQueryInfo.getShardingQueryInfoOrCoord(plan))
        if (childSInfos.forall(sI => sI.queryType == ReplicatedQuery)) {
          ShardQueryInfo.setShardingQueryInfo(op, shardedMD.REPLICATED_TABLE_INFO)
        } else {
          ShardQueryInfo.setShardingQueryInfo(op, shardedMD.COORD_QUERY_INFO)
        }
    }

    plan
  }

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan = {
    val oraCatalog =
      sparkSession.sessionState.catalogManager.catalog("oracle").asInstanceOf[OracleCatalog]
    implicit val shardedMD = oraCatalog.getMetadataManager.getShardingMetadata

    logDebug(s"applying AnnotateShardingInfoRule on ${plan.treeString}")

    plan transformUp {
      case ds @ DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
        annotate(ds)
        ds
    }
  }
}
