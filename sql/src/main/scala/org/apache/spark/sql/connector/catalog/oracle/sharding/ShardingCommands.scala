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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.connector.catalog.oracle.{OracleCatalog, OracleMetadata, OracleTable}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{IntegerType, StringType}

trait ShardingCommands extends RunnableCommand with Logging {

  protected def oracleCatalog(sparkSession: SparkSession): OracleCatalog =
    sparkSession.sessionState.catalogManager.catalog("oracle").asInstanceOf[OracleCatalog]

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val oraCatalog = oracleCatalog(sparkSession)
    if (!oraCatalog.getMetadataManager.isSharded) {
      Seq.empty
    } else {
      _run(sparkSession, oraCatalog.getMetadataManager.getShardingMetadata)
    }
  }

  protected def _run(sparkSession: SparkSession, shardingMetadata: ShardingMetadata): Seq[Row]
}

case object ListShardInstances extends ShardingCommands {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("name", StringType, nullable = false)(),
    AttributeReference("connectString", StringType, nullable = false)())

  override protected def _run(
      sparkSession: SparkSession,
      shardingMetadata: ShardingMetadata): Seq[Row] = {
    val shardInstances = shardingMetadata.shardInstances

    for (sI <- shardInstances) yield {
      Row(sI.name, sI.connectString)
    }
  }
}

case object ListTableFamilies extends ShardingCommands {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("id", IntegerType, nullable = false)(),
    AttributeReference("rootTable", StringType, nullable = false)(),
    AttributeReference("superShardType", StringType, nullable = false)(),
    AttributeReference("shardType", StringType, nullable = false)(),
    AttributeReference("superShardKeyCols", StringType, nullable = false)(),
    AttributeReference("shardKeyCols", StringType, nullable = false)(),
    AttributeReference("version", IntegerType, nullable = false)(),
  )

  override protected def _run(
      sparkSession: SparkSession,
      shardingMetadata: ShardingMetadata): Seq[Row] = {

    val tblFamilies = shardingMetadata.tableFamilies

    for (tF <- tblFamilies.values.toSeq) yield {
      Row(
        tF.id,
        tF.rootTable.toString(),
        tF.superShardType.toString,
        tF.shardType.toString,
        tF.superShardKeyCols.mkString("[", ",", "]"),
        tF.shardKeyCols.mkString("[", ",", "]"),
        tF.version)
    }
  }
}

case object ListReplicatedTables extends ShardingCommands {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("schema", StringType, nullable = false)(),
    AttributeReference("table", StringType, nullable = false)()
  )

  override protected def _run(
      sparkSession: SparkSession,
      shardingMetadata: ShardingMetadata): Seq[Row] = {

    val replicatedTables = shardingMetadata.replicatedTables.toSeq

    for (qNm <- replicatedTables) yield {
      Row(qNm.database, qNm.name)
    }
  }
}

case object ListShardedTables extends ShardingCommands {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("schema", StringType, nullable = false)(),
    AttributeReference("table", StringType, nullable = false)(),
    AttributeReference("tableFamilyId", IntegerType, nullable = false)(),
    AttributeReference("superKeyColumns", StringType, nullable = false)(),
    AttributeReference("keyColumns", StringType, nullable = false)()
  )

  override protected def _run(
      sparkSession: SparkSession,
      shardingMetadata: ShardingMetadata): Seq[Row] = {

    val shardTables = shardingMetadata.shardTables.values.toSeq

    for (sT <- shardTables) yield {
      Row(sT.qName.database, sT.qName.name, sT.tableFamilyId,
        sT.superKeyColumns.map(_.name).mkString("[", ",", "]"),
        sT.keyColumns.map(_.name).mkString("[", ",", "]")
      )
    }
  }
}

case class ListRoutingTable(oracleTable: OracleTable) extends ShardingCommands {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("interval_start", StringType, nullable = false)(),
    AttributeReference("interval_end", StringType, nullable = false)(),
    AttributeReference("shard_instances", StringType, nullable = false)()
  )

  override protected def _run(
      sparkSession: SparkSession,
      shardingMetadata: ShardingMetadata): Seq[Row] = {
    val oraTable = oracleTable.oraTable
    val qNm = QualifiedTableName(oraTable.schema, oraTable.name)
    val shardTable = shardingMetadata.shardTables.getOrElse(qNm,
      OracleMetadata.invalidAction(
        s"""Request routing for non sharded Table '$qNm'""".stripMargin,
        Some("""To see the routing table, you must provided a sharded table""".stripMargin)
      )
    )

    val routingTab = shardingMetadata.routingTables(shardTable.tableFamilyId)

    val res = routingTab.chunkRoutingIntervalTree.iterator.toSeq

    for (row <- res) yield {
      val int = row.interval
      val shards = row.value.map(i => shardingMetadata.shardInstances(i).name)
      Row(
        int.start.toString,
        int.end.toString,
        shards.mkString("[", ", ", "]")
      )
    }
  }
}

case object ListShardExecutorAffinity extends ShardingCommands {

  override val output: Seq[Attribute] = Seq.empty

  override protected def _run(
      sparkSession: SparkSession,
      shardingMetadata: ShardingMetadata): Seq[Row] = Seq.empty
}
