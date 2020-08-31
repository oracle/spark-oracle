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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.connector.catalog.oracle.sharding.ShardQueryInfo._

/**
 *  - child with [[ShardedQuery]]
 *   - try to convert groupingKey expressions into Sharding Keys
 *   - If we find an expression which is the 1st component(index == 0) of some
 *     ShardTable's shardingKey, we can maintain the sharding of the child.
 *   - Otherwise if grouping expressions are on Replicated Tables
 *     we can maintain the sharding of the child.
 *   - Otherwise this is a COORD_QUERY_INFO
 *  - otherwise, Aggregation takes on Sharding of child Operator.
 */
trait AggregateAnnotate { self: AnnotateShardingInfoRule.type =>

  private[sharding] def aggregate(from: LogicalPlan, aggOp: Aggregate)(
      implicit sparkSession: SparkSession,
      shardedMD: ShardingMetadata): Unit = {
    val sInfo = getShardingQueryInfoOrCoord(from)

    if (sInfo.queryType == ShardedQuery) {
      val conds = aggOp.groupingExpressions
      val shardingKeys: Seq[(OpCode, ShardTable, Int)] =
        conds.flatMap(c => toShardingKey(c, aggOp).toSeq)

      if (shardingKeys.exists(s => s._3 == 0)) {
        ShardQueryInfo.setShardingQueryInfo(aggOp, sInfo)
      } else {
        if (conds.nonEmpty && conds.forall(c => isGroupingOnReplTable(c, aggOp))) {
          // TODO: can introduce eager aggregation here.
          ShardQueryInfo.setShardingQueryInfo(aggOp, shardedMD.COORD_QUERY_INFO)
        } else {
          ShardQueryInfo.setShardingQueryInfo(aggOp, shardedMD.COORD_QUERY_INFO)
        }
      }
    } else {
      ShardQueryInfo.setShardingQueryInfo(aggOp, sInfo)
    }
  }

}
