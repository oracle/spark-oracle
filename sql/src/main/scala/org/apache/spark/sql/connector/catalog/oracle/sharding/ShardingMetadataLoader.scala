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

import scala.collection.mutable.ArrayBuffer

import oracle.spark.{ConnectionManagement, DataSourceKey}
import oracle.spark.sharding.ORAShardSQLs

import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata

trait ShardingMetadataLoader {

  def coordDSKey: DataSourceKey
  lazy val coordDSInfo = ConnectionManagement.info(coordDSKey)

  def constructShardingMetadata: ShardingMetadata = {
    val shardInstances = loadInstances
    val replicatedTables = loadReplicatedTableList
    val tableFamilies = {
      val m = loadTableFamilies
      m.mapValues { tf =>
        loadTableFamilyColumns(tf)
      }
    }
    new ShardingMetadata(coordDSKey, shardInstances, tableFamilies, replicatedTables)
  }

  private def loadInstances: Array[ShardInstance] = {

    ORAShardSQLs.listShardInstances(coordDSKey) { rs =>
      val buf = ArrayBuffer[ShardInstance]()
      while (rs.next()) {
        val name: String = rs.getString(1)
        val connectString: String = rs.getString(2)
        val shardURL = ShardingMetadata.shardURL(connectString)
        val shardInst = ShardInstance(name, connectString, coordDSInfo.convertToShard(shardURL))
        ConnectionManagement.registerDataSource(
          shardInst.shardDSInfo.connInfo,
          shardInst.shardDSInfo.catalogOptions
        )
        buf += shardInst
      }
      buf.toArray
    }
  }

  private def loadReplicatedTableList: Set[QualifiedTableName] = {
    ORAShardSQLs.listReplicatedTables(coordDSKey) { rs =>
      val buf = ArrayBuffer[QualifiedTableName]()
      while (rs.next()) {
        val owner: String = rs.getString(1)
        val tName: String = rs.getString(2)
        buf += QualifiedTableName(owner, tName)
      }
      buf.toSet
    }
  }

  private def loadTableFamilies: Map[Int, TableFamily] = {
    ORAShardSQLs.listTableFamilies(coordDSKey) { rs =>
      val buf = ArrayBuffer[(Int, TableFamily)]()
      while (rs.next()) {
        val id = rs.getInt(1)
        val superShardType = ShardingType(rs.getString(4))
        val shardType = ShardingType(rs.getString(6))
        val rootTable = QualifiedTableName(rs.getString(3), rs.getString(2))
        val numSuperShardingKeyCols = rs.getInt(5)
        val numShardingKeyCols = rs.getInt(7)
        val version = rs.getInt(8)

        buf += (
          (
            id,
            TableFamily(
              id,
              superShardType,
              shardType,
              rootTable,
              numSuperShardingKeyCols,
              numShardingKeyCols,
              Array.empty,
              Array.empty,
              version)))
      }
      buf.toMap
    }
  }

  private def loadTableFamilyColumns(tblFamily: TableFamily): TableFamily = {
    ORAShardSQLs.listTableFamilyColumns(coordDSKey, tblFamily.id) { rs =>
      val shardCols = ArrayBuffer[String]()
      while (rs.next()) {
        shardCols += rs.getString(3)
      }

      if (shardCols.size != tblFamily.numSuperShardingKeyCols + tblFamily.numShardingKeyCols) {
        OracleMetadata.invalidAction(
          s"""Loading Table Family(root table = ${tblFamily.rootTable})
             | failed to load sharding column metadata""".stripMargin,
          None)
      }

      tblFamily.copy(
        superShardKeyCols = shardCols.slice(0, tblFamily.numSuperShardingKeyCols).toArray,
        shardKeyCols = shardCols.slice(tblFamily.numSuperShardingKeyCols, shardCols.size).toArray)
    }
  }

  private[sharding] def loadTableFamilyChunks(tblFamily: TableFamily): Array[ChunkInfo] = {
    ORAShardSQLs.listTableFamilyChunks(coordDSKey, tblFamily.id) { rs =>
      val chunks = ArrayBuffer[ChunkInfo]()
      while (rs.next()) {
        chunks += ChunkInfo(
          rs.getString(1),
          rs.getBytes(2),
          rs.getBytes(3),
          rs.getBytes(4),
          rs.getBytes(5),
          rs.getInt(6),
          rs.getInt(7),
          rs.getInt(8),
          rs.getString(9),
          rs.getInt(10),
          rs.getInt(11)
        )
      }

      chunks.toArray
    }
  }

}
