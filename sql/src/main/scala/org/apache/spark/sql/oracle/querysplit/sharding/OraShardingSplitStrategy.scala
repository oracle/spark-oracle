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

package org.apache.spark.sql.oracle.querysplit.sharding

import scala.util.Random

import oracle.spark.DataSourceKey

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.operators.{OraPlan, OraTableScan}
import org.apache.spark.sql.oracle.querysplit.{OraDBSplit, OraNoSplit, OraSplitStrategy, PlanInfo}

case class ShardQueryDBSplit(shardIdx : Int,
                             shardSplitId : Int) extends OraDBSplit {
  def explain(append : String => Unit) : Unit = ()
}

case object CoordinatorQuerySplitStrategy extends OraSplitStrategy {
  val splitList = IndexedSeq(OraNoSplit)

  override def splitOraSQL(oraTblScan: OraTableScan, splitId : Int): Option[SQLSnippet] =
    None

  def explain(append: String => Unit): Unit =
    append("Query executed on Shard Coordinator\n")
}

case class ReplicatedQuerySplitStrategy(shardInstanceDSKey : DataSourceKey,
                                        shardSplitStrategy : OraSplitStrategy
                                  ) extends OraSplitStrategy {

  override def dskey(planningDSKey : DataSourceKey, splitId : Int) : DataSourceKey =
    shardInstanceDSKey

  override val splitList: IndexedSeq[OraDBSplit] = shardSplitStrategy.splitList

  override def splitOraSQL(oraTblScan: OraTableScan, splitId: Int): Option[SQLSnippet] =
    shardSplitStrategy.splitOraSQL(oraTblScan, splitId)

  override def preferredLocs(splitId : Int): Array[String] =
    OraShardingSplitStrategy.preferredLocs(shardInstanceDSKey)

  override def explain(append: String => Unit): Unit = {
    append(s"Replicated Query is run on instance: ${shardInstanceDSKey}")
    append(s"\nShard Instance ${shardInstanceDSKey} Split Strategy:\n")
    shardSplitStrategy.explain(append)
  }

  override def associateFetchClause(sqlSnip: SQLSnippet,
                           addOrderBy : Boolean,
                           orderByCnt : Int,
                           splitId : Int) : SQLSnippet =
    shardSplitStrategy.associateFetchClause(sqlSnip, addOrderBy, orderByCnt, splitId)
}

case class ShardQuerySplitStrategy(shardSplitStrategies :
                                   IndexedSeq[(OraSplitStrategy, Option[PlanInfo])],
                                   shardDSKeys : IndexedSeq[DataSourceKey]
                             ) extends OraSplitStrategy {

  override val splitList: IndexedSeq[ShardQueryDBSplit] =
    (for (( (shardStrat, _), i) <- shardSplitStrategies.zipWithIndex) yield {
      shardStrat.splitIds.map(sSId => ShardQueryDBSplit(i, sSId))
    }).flatten

  override def dskey(planningDSKey : DataSourceKey, splitId : Int) : DataSourceKey =
    shardDSKeys(splitList(splitId).shardIdx)

  override def splitOraSQL(oraTblScan: OraTableScan, splitId: Int): Option[SQLSnippet] = {
    val shardSplit = splitList(splitId)
    shardSplitStrategies(shardSplit.shardIdx)._1.splitOraSQL(oraTblScan, shardSplit.shardSplitId)
  }

  override def preferredLocs(splitId : Int): Array[String] =
    OraShardingSplitStrategy.preferredLocs(shardDSKeys(splitList(splitId).shardIdx))

  override def explain(append: String => Unit): Unit = {
    append(s"Sharded Query is run on ${shardDSKeys.size} instances")
    for((shardDSKey, (shardStrat, planInfo)) <- shardDSKeys.zip(shardSplitStrategies)) {
      append(s"\nShard Instance ${shardDSKey} Split Strategy:\n")
      if (planInfo.isDefined) {
        append("Pushdown Oracle SQL, oracle plan stats estimates:\n")
        planInfo.get.explain(append)
      }
      shardStrat.explain(append)
    }
  }

  override def associateFetchClause(sqlSnip: SQLSnippet,
                                    addOrderBy : Boolean,
                                    orderByCnt : Int,
                                    splitId : Int) : SQLSnippet = {
    val shardSplit = splitList(splitId)
    shardSplitStrategies(shardSplit.shardIdx)._1.
      associateFetchClause(sqlSnip, addOrderBy, orderByCnt, shardSplit.shardSplitId)
  }
}

/**
 * - [[CoordinatorQuery]] is evaluated as a 'no split' query on the Coordinator instance.
 * - [[ReplicatedQuery]] is evaluated on a randomly picked Shard instance.
 *   An [[OraSplitStrategy]] is applied on the query execution on that instance.
 * - [[ShardedQuery]] is evaluated on all the [[ShardInstance shard instances]]  inferred
 *   from the query. An [[OraSplitStrategy]] is applied on the query execution on each instance.
 */
object OraShardingSplitStrategy extends Logging {

  private[sharding] val rand = new Random()

  private[sharding] def nextShardInstance(shardedMD : ShardingMetadata) : ShardInstance = {
    shardedMD.shardInstances(rand.nextInt(shardedMD.shardInstances.size))
  }

  /*
   * TODO: based on Spark cluster and Shard instance network topology provide
   *  a mapping between Shard instance and preferred locations.
   */
  private[sharding] def preferredLocs(shardInstanceDSKey : DataSourceKey
                                     ): Array[String] = Array.empty

  def generateSplits(dsKey : DataSourceKey,
                     oraPlan : OraPlan,
                     shardQInfo : ShardQueryInfo,
                     shardedMD : ShardingMetadata)(
                      implicit sparkSession : SparkSession
                    ) : (OraSplitStrategy, Option[PlanInfo]) = {

    logDebug(
      s"""OraShardingSplitStrategy:generateSplits
         |  dsKey : ${dsKey}
         |  shardQInfo : ${shardQInfo}
         |""".stripMargin
    )

    shardQInfo.queryType match {
      case CoordinatorQuery => (CoordinatorQuerySplitStrategy, shardQInfo.planInfo)
      case ReplicatedQuery =>
        val shardInstance = OraShardingSplitStrategy.nextShardInstance(shardedMD)
        val shardInstanceDSKey = shardInstance.shardDSInfo.key
        val (shardSplitStrategy, planInfo) =
          OraSplitStrategy.generateSplits(shardInstanceDSKey, oraPlan)(sparkSession)
        (ReplicatedQuerySplitStrategy(shardInstanceDSKey, shardSplitStrategy), planInfo)
      case ShardedQuery =>
        val (shardSplitStrategies : IndexedSeq[(OraSplitStrategy, Option[PlanInfo])],
        shardDSKeys : IndexedSeq[DataSourceKey]) =
          (for (sIdx <- shardQInfo.shardInstances.toIndexedSeq;
                shardInst = shardedMD.shardInstances(sIdx);
                shardDSKey = shardInst.shardDSInfo.key
                ) yield {
            (OraSplitStrategy.generateSplits(shardDSKey, oraPlan)(sparkSession), shardDSKey)
          }).unzip
        (ShardQuerySplitStrategy(shardSplitStrategies, shardDSKeys), None)
    }
  }

}