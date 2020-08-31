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

import scala.collection.mutable.{Map => MMap}

import oracle.spark.{DataSourceInfo, DataSourceKey}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.oracle.{OracleMetadata, OracleMetadataManager}
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.{OraColumn, OraForeignKey, OraTable}
import org.apache.spark.sql.connector.catalog.oracle.sharding.routing.{RoutingQueryInterface, RoutingTable}

case class ShardInstance(name: String, connectString: String, shardDSInfo: DataSourceInfo)

case class ChunkInfo(
    shardName: String,
    shardKeyLo: Array[Byte],
    shardKeyHi: Array[Byte],
    groupKeyLo: Array[Byte],
    groupKeyHi: Array[Byte],
    chunkId: Int,
    groupId: Int,
    uniqueId: Int,
    name: String,
    priority: Int,
    state: Int)

case class ShardTable(
    qName: QualifiedTableName,
    tableFamilyId: Int,
    superKeyColumns: Array[OraColumn],
    keyColumns: Array[OraColumn]) {

  def numShardingKeys : Int = superKeyColumns.size + keyColumns.size

  val columnIndexMap = CaseInsensitiveMap(
    ((superKeyColumns ++ keyColumns).zipWithIndex.map {
    case (oc, i) => oc.name -> i
  }).toMap
  )

  def isShardingKey(ar : AttributeReference) : Option[Int] = {
    columnIndexMap.get(ar.name)
  }
}

case class TableFamily(
    id: Int,
    superShardType: ShardingType,
    shardType: ShardingType,
    rootTable: QualifiedTableName,
    numSuperShardingKeyCols: Int,
    numShardingKeyCols: Int,
    superShardKeyCols: Array[String],
    shardKeyCols: Array[String],
    version: Int)

/*
 * attached to OracleCatalog through an Atomic reference
 * - loaded on Catalog setup & on issue of reload
 * - Routing information per tblFamily loaded on demand or explicit show command
 *
 * Supported Commands:
 * - list Shard Instances
 * - list Table Families
 * - list replicated Tables
 * - Reload Sharding Metadata
 * - Reload Routing for Table
 * - List Routing for Table
 * - List Executor-Shard Affinity
 *
 */

/**
 * A cache of:
 *  - table families in an instance
 *  - replicated tables in a Sharded instance
 *  - [[ShardTable]] information for sharded tables.
 *    These are loaded on first access of the Sharded Table.
 *  - [[RoutingTable]] for table families. This is loaded on first access of the root table
 *    of the Table family.
 *
 * @param coordDSKey
 * @param shardInstances
 * @param tableFamilies
 * @param replicatedTables
 */
class ShardingMetadata private[sharding] (
    val coordDSKey: DataSourceKey,
    val shardInstances: Array[ShardInstance],
    val tableFamilies: Map[Int, TableFamily],
    val replicatedTables: Set[QualifiedTableName]) extends ShardingMetadataLoader {

  private[sharding] val rootTblFamilyMap: Map[QualifiedTableName, TableFamily] = tableFamilies.map {
    case (id, tF) => (tF.rootTable, tF)
  }

  private[sharding] val shardTables = MMap[QualifiedTableName, ShardTable]()
  private[sharding] val routingTables = MMap[Int, RoutingTable]()

  private def findShardingColumn(cNm : String, oraTable: OraTable) : OraColumn = {
    oraTable.columns.find(c => c.name == cNm).getOrElse {
      OracleMetadata.invalidAction(
        s"""Loading Sharding metadata for table '${oraTable.name}'
           | failed to locate sharding column ${cNm} in table schema""".stripMargin,
        None
      )
    }
  }

  private def validateShardingFK(tblQualNm : QualifiedTableName,
                                  parentShardTable: ShardTable,
                                 shardingFK : OraForeignKey) : Unit = {

    val parentShardCols = parentShardTable.superKeyColumns ++ parentShardTable.keyColumns
    val fkCols = shardingFK.referencedCols.tail

    def throwError : Nothing =
      OracleMetadata.invalidAction(
        s"""Loading Sharding metadata for table '${tblQualNm}'
           | failed to match Sharding FK columns:
           |     ${fkCols.mkString(",")}
           | against Parent Table(${parentShardTable.qName}) shard columns:
           |     ${parentShardCols.map(_.name).mkString(",")}""".stripMargin,
        None
      )


    if (fkCols.size != parentShardCols.size) {
      throwError
    }

    for((pShardCol, fkCOl) <- parentShardCols.zip(fkCols)) {
      if (pShardCol.name != fkCOl) throwError
    }
  }

  private def loadRootShardTable(tabFamily: TableFamily,
                                 oraTable: OraTable): Unit = {

    val superKeyColumns: Array[OraColumn] =
      tabFamily.superShardKeyCols.map(cNm => findShardingColumn(cNm, oraTable))
    val keyColumns: Array[OraColumn] =
      tabFamily.shardKeyCols.map(cNm => findShardingColumn(cNm, oraTable))
    val shardTable = ShardTable(
      QualifiedTableName(oraTable.schema, oraTable.name),
      tabFamily.id,
      superKeyColumns,
      keyColumns
    )

    val chunks: Array[ChunkInfo] = loadTableFamilyChunks(tabFamily)
    val rTable = RoutingTable(tabFamily, shardTable, shardInstances, chunks)

    shardTables(shardTable.qName) = shardTable
    routingTables(tabFamily.id) = rTable

  }

  def loadNonRootShardTable(parentShardTable: ShardTable,
                            shardingFK : OraForeignKey,
                            oraTable: OraTable): Unit = {
    val qualNm = QualifiedTableName(oraTable.schema, oraTable.name)

    validateShardingFK(qualNm, parentShardTable, shardingFK)

    val shardingCols = shardingFK.cols.tail
    val superShardColNms = shardingCols.take(parentShardTable.superKeyColumns.size)
    val shardColNms = shardingCols.drop(parentShardTable.superKeyColumns.size)
    val superKeyColumns: Array[OraColumn] =
      superShardColNms.map(cNm => findShardingColumn(cNm, oraTable))
    val keyColumns: Array[OraColumn] =
      shardColNms.map(cNm => findShardingColumn(cNm, oraTable))

    val shardTable = ShardTable(
      QualifiedTableName(oraTable.schema, oraTable.name),
      parentShardTable.tableFamilyId,
      superKeyColumns,
      keyColumns
    )
    shardTables(shardTable.qName) = shardTable

  }

  /**
   * case table is known replicate || sharded => do nothing
   * otherwise
   *   construct `ShardingTableChecks(oraTable)`
   *   case table is not sharded => do nothing
   *   case table is a Root Shard Table =>
   *     get its tableFamily from `rootTblFamilyMap`
   *     invoke `loadRootShardTable(tblFamily, oraTable)`
   *   case table is non-root Shard Table =>
   *     extract its parent table from OraTable foreign key relations
   *     recursively call `oraMDMgr.oraTable(parentTable.database, parentTable.name)`
   *       to ensure parent table sharding information is in cache
   *     invoke `loadNonRootShardTable(parentShardTable, oraTable)`
   *
   * @param oraTable
   * @param oraMDMgr
   */
  private[oracle] def registerTable(oraTable: OraTable, oraMDMgr: OracleMetadataManager): Unit =
    synchronized {
      val qNm = QualifiedTableName(oraTable.schema, oraTable.name)

      if (replicatedTables.contains(qNm) || shardTables.contains(qNm)) {
        ()
      } else {
        import ShardingMetadata._

        val shardingChecks = ShardingTableChecks(oraTable)

        if (!shardingChecks.isShardedTable) {
          ()
        } else {

          if (shardingChecks.isRootTable) {
            val tblFamily = rootTblFamilyMap.getOrElse(
              qNm,
              OracleMetadata.invalidAction(
                s"""Loading Sharding metadata for table '$qNm'
                 | failed to setup as root of a table family""".stripMargin,
                None))
            loadRootShardTable(tblFamily, oraTable)
          } else {
            val prntTabNm: QualifiedTableName = shardingChecks.parentTable.getOrElse {
              OracleMetadata.invalidAction(
                s"""Loading Sharding metadata for table '$qNm'
                 | failed to locate foreign key based sharding association""".stripMargin,
                Some("""Currently we only support loading sharding metadata
                  |for foreign key based sharding association""".stripMargin))
            }

            val parentShardTable: ShardTable = shardTables.getOrElse(prntTabNm, {
              oraMDMgr.oraTable(prntTabNm.database, prntTabNm.name)
              shardTables(prntTabNm)
            })
            loadNonRootShardTable(parentShardTable, shardingChecks.shardingForeignKey.get, oraTable)
          }
        }
      }
    }

  private[oracle] def invalidateTable(schema: String, table: String): Unit = synchronized {
    val qualNm = QualifiedTableName(schema, table)
    if (!shardTables.contains(qualNm)) {
      ()
    } else {
      // this table is sharded, but not the root of a table Family
      if (!rootTblFamilyMap.contains(qualNm)) {
        shardTables.-(qualNm)
      } else { // root table of a table Family

        val sTbl = shardTables(qualNm)
        val tableFamilyId = sTbl.tableFamilyId
        var sTabs = Set.empty[QualifiedTableName]
        for((k, v) <- shardTables if v.tableFamilyId == tableFamilyId) {
          sTabs += k
        }
        // remove all tables in the family and the routing table for the family
        shardTables.--(sTabs)
        routingTables.-(tableFamilyId)
      }
    }
  }

  private val ALL_SHARD_INSTANCES = Range(0, shardInstances.size).toSet

  private[sql] val REPLICATED_TABLE_INFO =
    ShardQueryInfo(ReplicatedQuery, Set.empty, ALL_SHARD_INSTANCES, None)
  private[sql] val COORD_QUERY_INFO = ShardQueryInfo(CoordinatorQuery, Set.empty, Set.empty, None)

  private[sql] def shardQueryInfo(oraTable : OraTable) : ShardQueryInfo = {
    val qualifiedTableName = QualifiedTableName(oraTable.schema, oraTable.name)

    if (replicatedTables.contains(qualifiedTableName)) {
      REPLICATED_TABLE_INFO
    } else {
      val sTbl = shardTables.get(qualifiedTableName)
      if (sTbl.isDefined) {
        ShardQueryInfo(ShardedQuery, sTbl.toSet, ALL_SHARD_INSTANCES, None)
      } else {
        COORD_QUERY_INFO
      }
    }
  }

  private[sql] def getRoutingTable(shardTbl : ShardTable) : RoutingQueryInterface = {
    routingTables(shardTbl.tableFamilyId)
  }

  private[sql] def getShardTable(qNm : QualifiedTableName) : ShardTable = shardTables(qNm)

}

object ShardingMetadata {

  trait ShardingException extends AnalysisException

  class UnsupportedShardingType(shardType: String, reason: Option[String])
      extends AnalysisException(
        s"Unsupported ShardType ${shardType}" +
          s" ${if (reason.isDefined) "," + reason.get else ""}")

  def unsupportedShardType(typeNm: String, reason: Option[String] = None): Nothing =
    throw new UnsupportedShardingType(typeNm, reason)

  def shardURL(shardConnectStr: String): String = s"jdbc:oracle:thin:@//${shardConnectStr}"

  private val SHARDING_HIDDEN_COLUMN_NAME = "SYS_HASHVAL"

  private case class ShardingTableChecks(oraTable: OraTable) {
    lazy val shardingSysCol: Option[OraColumn] = oraTable.columns.find { c =>
      c.name == SHARDING_HIDDEN_COLUMN_NAME
    }
    lazy val isShardedTable = shardingSysCol.isDefined

    lazy val shardingForeignKey = oraTable.foreignKeys.find { fk =>
      fk.cols.contains(SHARDING_HIDDEN_COLUMN_NAME)
    }
    lazy val isRootTable = !shardingForeignKey.isDefined
    lazy val parentTable: Option[QualifiedTableName] = shardingForeignKey.map(fk =>
      QualifiedTableName(fk.referencedTable._1, fk.referencedTable._2))
  }

}
