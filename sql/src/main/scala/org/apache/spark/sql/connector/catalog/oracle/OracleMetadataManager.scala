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

package org.apache.spark.sql.connector.catalog.oracle

import java.io.File
import java.util.Locale

import oracle.spark.{ConnectionInfo, ConnectionManagement, DataSourceKey, ORAMetadataSQLs}
import oracle.spark.datastructs.SQLIdentifierMap
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.{DB, Options}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.{OraIdentifier, OraTable}
import org.apache.spark.sql.connector.catalog.oracle.sharding.{ShardingMetadata, ShardingMetadataLoader}
import org.apache.spark.sql.oracle.OracleCatalogOptions
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Provides Oracle Metadata. Clients can ask for ''namespaces'',
 * ''tables'' and table details as ''OraTable''.
 *
 * It can operate in ''cache_only'' mode where it serves request from information in the
 * local cache. This should only be used for testing.
 * Update on 3/24/21:
 * - cache_only mode making less sense, now that tests need a Connection.
 * - tests now load missing table metadata from DB,
 *   so only namespaces and tablespaces calls strictly served from cache.
 *
 * Caching behavior:
 * - on startup namespace list and table lists are loaded from local disk cache or DB
 *   and cached in memory.
 * - table metadata is loaded into local disk cache on demand.
 * - steady state is served from in-memory namespace & table lists + on-disk cached table metadata
 * - 'invalidateTable' deletes on-disk cached table metadata.
 * - 'reloadCatalog' reload in-memory and on-disk cache of namespace list and table lists
 *   - This doesn't clear individual table metadata
 *
 * Caching Update on 4/28, with support for Sharded Instances.
 *  - There are several cache structures and types.
 *    - The ''Level DB'' persistent cache stores [[OraTable]] structures and
 *      can be reused across restarts. a `invalidateTable` can clear entry for
 *      a table in this cache.
 *    - The in memory caches spread between [[OracleMetadataManager]] and [[ShardingMetadata]]
 *      are loaded on Catalog init and can be reloaded via the `loadCatalog(reloadFromDB=true)`
 *      call.
 *  - A Table's metadata is cached here in the ''Level DB'' cache of [[OraTable]] and the
 *    [[org.apache.spark.sql.connector.catalog.oracle.sharding.ShardTable]] in [[ShardingMetadata]]
 *  - There is very minimal consistency guarantees across caches. [[ShardingMetadata]] has
 *    a synchronize method, caches in [[OracleMetadataManager]] have no locking.
 *    The assumption is that cache inconsistencies can be overcome by a `reloadCatalog` and
 *    `invalidateTable` calls.
 *  - This means caching behavior is complex and hard to explain to users, with many ways to get
 *    into bad states.
 *  - The reasons for providing a persistent cache are less relevant now that we maintain a
 *    connection to the Oracle instance.
 *  - TODO
 *    - remove the persistent cache and move to a single in memory cache.
 *    - always loadFromDB
 *    - a `loadCatalog` clears everything in the cache.
 *
 * @param cMap
 */
private[oracle] class OracleMetadataManager(cMap: CaseInsensitiveMap[String])
  extends Logging with OraFunctionDefLoader with OraTypes {

  val connInfo = ConnectionInfo.connectionInfo(cMap)
  val catalogOptions = OracleCatalogOptions.catalogOptions(cMap)

  val cache_only = catalogOptions.metadata_cache_only

  val dsKey: DataSourceKey = {
    val dsKey = ConnectionManagement.registerDataSource(connInfo, catalogOptions)
    ORAMetadataSQLs.validateConnection(dsKey)
    dsKey
  }

  private val cacheLoc: File =
    catalogOptions.metadataCacheLoc.map(new File(_)).getOrElse(Utils.createTempDir())

  private val cache: DB = {
    val options = new Options
    options.createIfMissing(true)
    val db = JniDBFactory.factory.open(cacheLoc, options)

    log.info(s"Opened metadata_cache at ${cacheLoc.getAbsolutePath}")

    ShutdownHookManager.addShutdownHook { () =>
      log.info(s"closing metadata_cache at ${cacheLoc.getAbsolutePath}")
      db.close()
    }
    db
  }

  @volatile private var _namespacesMap: SQLIdentifierMap[String] = null
  @volatile private var _namespaces : Array[Array[String]] = null
  @volatile private var _tableMap: SQLIdentifierMap[Set[String]] = null
  @volatile private var shardingMetadata : Option[ShardingMetadata] = None

  private[oracle] val defaultNamespace: String = dsKey.userName

  /*
   * On start load Catalog
   */
  loadCatalog(true)

  private def loadNamespaces(reloadFromDB : Boolean = false) : Unit = {

    val nsBytes : Array[Byte] = if (!reloadFromDB) {
      cache.get(OracleMetadata.NAMESPACES_CACHE_KEY)
    } else null

    val nsSet = if (nsBytes != null) {
      Serialization.deserialize[Set[String]](nsBytes)
    } else {
      val accessibleUsers = ORAMetadataSQLs.listAccessibleUsers(dsKey).toSet

      cache.put(
        OracleMetadata.NAMESPACES_CACHE_KEY,
        Serialization.serialize[Set[String]](accessibleUsers))
      accessibleUsers
    }
    _namespacesMap = SQLIdentifierMap(nsSet.map(n => n -> n).toMap)
    _namespaces = _namespacesMap.values.map(ns => Array(ns)).toArray
  }

  private def loadTableSpaces(reload : Boolean = false) : Unit = {

    val tListBytes : Array[Byte] = if (!reload) {
      cache.get(OracleMetadata.TABLE_LIST_CACHE_KEY)
    } else null

    val tablMap: Map[String, Array[String]] = if (tListBytes != null) {
      Serialization.deserialize[Map[String, Array[String]]](tListBytes)
    } else {
      val tblMap = ORAMetadataSQLs.listAllTables(dsKey)

      cache.put(
        OracleMetadata.TABLE_LIST_CACHE_KEY,
        Serialization.serialize[Map[String, Array[String]]](tblMap))
      tblMap
    }
    _tableMap = SQLIdentifierMap(tablMap.mapValues(_.toSet))
  }


  private[oracle] def namespaces : Array[Array[String]] = {
    _namespaces
  }

  private[oracle] def namespaceExists(ns: String): Boolean = {
    _namespacesMap.contains(ns)
  }

  private[oracle] def tableMap : SQLIdentifierMap[Set[String]] = {
    _tableMap
  }

  private def tableKey(schema: String, table: String): (String, String, Array[Byte]) = {
    val oraSchema = _namespacesMap(schema)
    val oraTblNm = if (_tableMap(oraSchema).contains(table)) {
      table
    } else table.toUpperCase(Locale.ROOT)

    val tblId = OraIdentifier(Array(oraSchema), oraTblNm)
    val tblIdKey = Serialization.serialize(tblId)
    (oraSchema, oraTblNm, tblIdKey)
  }

  private[oracle] def oraTable(schema: String, table: String): OraTable = {
    val (oraSchema, oraTblNm, tblIdKey) = tableKey(schema, table)
    val tblMetadataBytes = cache.get(tblIdKey)

    val oraTbl = if (tblMetadataBytes != null) {
      Serialization.deserialize[OraTable](tblMetadataBytes)
    } else {
      val _oraTbl = oraTableFromDB(oraSchema, oraTblNm)
      val tblMetadatBytes = Serialization.serialize(_oraTbl)
      cache.put(tblIdKey, tblMetadatBytes)
      _oraTbl
    }

    if (shardingMetadata.isDefined) {
      shardingMetadata.get.registerTable(oraTbl, this)
    }

    oraTbl

  }

  private def oraTableFromDB(schema: String, table: String): OraTable = {
    val (xml, sxml) = ORAMetadataSQLs.tableMetadata(dsKey, schema, table)
    XMLReader.parseTable(xml, sxml)
  }

  private[oracle] def invalidateTable(schema: String, table: String): Unit = {
    if (shardingMetadata.isDefined) {
      shardingMetadata.get.invalidateTable(schema, table)
    }

    if (!cache_only) {
      val (_, _, tblIdKey) = tableKey(schema, table)
      cache.delete(tblIdKey)
    }
  }

  private[oracle] def loadCatalog(reloadFromDB : Boolean = true) : Unit = {
    loadNamespaces(reloadFromDB)
    loadTableSpaces(reloadFromDB)

    val dsInfo = ConnectionManagement.info(dsKey)
    /*
     * For sharded instance
     * - load table families
     * - and then load the root tables of the families.
     */
    if (dsInfo.isSharded) {
      val shrdMDLoader = new ShardingMetadataLoader {
        val coordDSKey : DataSourceKey = dsKey
      }
      shardingMetadata = Some(shrdMDLoader.constructShardingMetadata)

      /*
       * Not loading the root tables on startup
       * Why?
       * - loading of OraTable leads to call to `OraDataType.create`
       *   which looks for `OracleCatalog` on the current Session.
       *   But we are in a call stack that is trying to setup the
       *   Oracle Catalog. So this leads to another attempt to create
       *   and initialize the Oracle Catalog. Which eventually fails
       *   when trying to load the level-db cache the second time.
       */
      if (false) {
        for (tblFam <- shardingMetadata.get.tableFamilies.values) {
          oraTable(tblFam.rootTable.database, tblFam.rootTable.name)
        }
      }
    }
  }

  def isSharded : Boolean = ConnectionManagement.info(dsKey).isSharded

  private[sql] def getShardingMetadata : ShardingMetadata = shardingMetadata.get

}
