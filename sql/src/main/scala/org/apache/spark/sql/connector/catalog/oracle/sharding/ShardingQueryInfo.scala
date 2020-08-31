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

package org.apache.spark.sql.connector.catalog.oracle.sharding

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.oracle.querysplit.PlanInfo

sealed trait ShardQueryType

case object ReplicatedQuery extends ShardQueryType
case object CoordinatorQuery extends ShardQueryType
case object ShardedQuery extends ShardQueryType

case class ShardQueryInfo(queryType : ShardQueryType,
                          shardTables : Set[ShardTable],
                          shardInstances : Set[Int],
                          planInfo : Option[PlanInfo]
                         ) {

  lazy val tableFamily = shardTables.headOption.map(_.tableFamilyId).getOrElse(-1)

  def show(append: String => Unit) : Unit = {
    append(queryType.toString)
    if (queryType == ShardedQuery) {
      append(s", shardTables = ${shardTables.map(_.qName).mkString("[", ", ", "]")}")
      append(s", tableFamily = ${tableFamily}")
      append(s", shardInstances = ${shardInstances.mkString("[", ", ", "]")}")
    }
  }

  def show : String = {
    val sb = new StringBuilder
    show( s => sb.append(s))
    sb.toString()
  }
}

object ShardQueryInfo {
  val ORA_SHARDING_QUERY_TAG = TreeNodeTag[ShardQueryInfo]("_ShardingQueryInfo")

  private[sql] def hasShardingQueryInfo(plan : TreeNode[_]) : Boolean =
    getShardingQueryInfo(plan).isDefined

  private[sql] def getShardingQueryInfo(plan : TreeNode[_]) : Option[ShardQueryInfo] =
    plan.getTagValue(ORA_SHARDING_QUERY_TAG)

  private[sql] def getShardingQueryInfoOrCoord(plan : TreeNode[_])
                                              (implicit sparkSession: SparkSession,
                                               shardedMD : ShardingMetadata
                                              ): ShardQueryInfo =
    plan.getTagValue(ORA_SHARDING_QUERY_TAG).getOrElse(shardedMD.COORD_QUERY_INFO)

  private[sql] def setShardingQueryInfo(plan : TreeNode[_], sInfo : ShardQueryInfo) : Unit =
    plan.setTagValue(ORA_SHARDING_QUERY_TAG, sInfo)
}
