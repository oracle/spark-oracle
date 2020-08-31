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

package oracle.spark

import java.sql.Connection
import java.util.concurrent.{ConcurrentHashMap => CMap}

import scala.language.implicitConversions

import oracle.spark.ORASQLUtils._
import oracle.spark.sharding.ORAShardSQLs

import org.apache.spark.sql.oracle.OracleCatalogOptions

case class DataSourceKey(connectionURL: String, userName: String) {
  def convertToShard(shardURL : String) : DataSourceKey = {
    this.copy(connectionURL = shardURL)
  }

}

object DataSourceKey {
  implicit def dataSourceKey(connInfo: ConnectionInfo): DataSourceKey =
    DataSourceKey(connInfo.url, connInfo.username)
}

case class DataSourceInfo(
    key: DataSourceKey,
    connInfo: ConnectionInfo,
    catalogOptions: OracleCatalogOptions,
    isSharded: Boolean) {

  def convertToShard(shardURL : String) : DataSourceInfo = {
    assert(isSharded)
    this.copy(
      key = key.convertToShard(shardURL),
      connInfo = connInfo.convertToShard(shardURL)
    )
  }

}

trait DataSources {

  private val dsMap = new CMap[DataSourceKey, DataSourceInfo]()

  private[oracle] def registerDataSource(
      dsKey: DataSourceKey,
      connInfo: ConnectionInfo,
      catalogOptions: OracleCatalogOptions): Unit = {
    val createDSI = new java.util.function.Function[DataSourceKey, DataSourceInfo] {
      override def apply(t: DataSourceKey): DataSourceInfo = {
        val isSharded = setupConnectionPool(dsKey, connInfo)
        DataSourceInfo(dsKey, connInfo, catalogOptions, isSharded)
      }
    }
    val dsInfo = dsMap.computeIfAbsent(dsKey, createDSI)
    if (dsInfo.connInfo != connInfo) {
      throwAnalysisException(s"""
           |Currently we require all table definitions to a database to have the
           |same connection properties:
           |Properties Already registered:
           |${dsInfo.connInfo.dump}
           |Properties specified:
           |${connInfo.dump}
         """.stripMargin)
    }
  }

  def registerDataSource(
      connInfo: ConnectionInfo,
      catalogOptions: OracleCatalogOptions): DataSourceKey = {
    val dsKey: DataSourceKey = connInfo
    registerDataSource(dsKey, connInfo, catalogOptions)
    dsKey
  }

  def info(dsKey: DataSourceKey): DataSourceInfo = {
    import scala.collection.JavaConverters._
    dsMap.asScala.getOrElse(
      dsKey,
      throwAnalysisException(s"Couldn't find details about DataSource ${dsKey}"))
  }

  private[oracle] def isSharded(dsKey: DataSourceKey): Boolean = {
    info(dsKey).isSharded
  }

  private[oracle] def getConnection(dsKey: DataSourceKey): Connection = {
    ConnectionManagement.getConnection(dsKey, info(dsKey).connInfo)
  }

  private def setupConnectionPool(dsKey: DataSourceKey, connInfo: ConnectionInfo): Boolean = {
    val b = withConnection[Boolean](
      dsKey,
      ConnectionManagement.getConnection(dsKey, connInfo),
      "setup connection pool") { conn =>
      ORAShardSQLs.isShardedInstance(conn)
    }
    b
  }

}
