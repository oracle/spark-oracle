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

package oracle.spark.sharding

import java.sql.{Connection, ResultSet, SQLException}

import oracle.spark.DataSourceKey
import oracle.spark.ORASQLUtils.{performDSQuery, performQuery}

object ORAShardSQLs {

  private val IS_SHARDED_INSTANCE_QUERY =
    """SELECT "NAME" from "GSMADMIN_INTERNAL"."DATABASE" """

  private val LIST_SHARD_INSTANCES =
    """
      |SELECT "NAME", "CONNECT_STRING"
      |from "GSMADMIN_INTERNAL"."DATABASE" """.stripMargin

  // MLOG$_<tNm>, RUPD$_<tNm>
  private val LIST_REPLICATED_TABLES =
  """with tlist as
    |(SELECT "OWNER",
    |       case
    |           when "TABLE_NAME" like 'RUPD$_%' then substr("TABLE_NAME", 7)
    |           when "TABLE_NAME" like 'MLOG$_%' then substr("TABLE_NAME", 7)
    |           else "TABLE_NAME"
    |           end tname
    |from "ALL_TABLES"
    |where "OWNER" in (select "USERNAME" from "ALL_USERS" where "ORACLE_MAINTAINED" = 'N')
    |)
    |select "OWNER", tname, count(*)
    |from tlist
    |group by "OWNER", tname
    |having count(*) = 3
    |order by 1, 2""".stripMargin

  val LIST_TABLE_FAMILIES =
    """
      |select "TABFAM_ID", "TABLE_NAME", "SCHEMA_NAME", "GROUP_TYPE",
      |       "GROUP_COL_NUM", "SHARD_TYPE", "SHARD_COL_NUM", "DEF_VERSION"
      |from "LOCAL_CHUNK_TYPES"
      |""".stripMargin

  val LIST_TABLE_FAMILY_COLUMNS =
  """
      |select "SHARD_LEVEL", "COL_IDX_IN_KEY", "COL_NAME"
      |from "LOCAL_CHUNK_COLUMNS"
      |where "TABFAM_ID" = ?
      |order by "SHARD_LEVEL", "COL_IDX_IN_KEY"
      |""".stripMargin

  val LIST_CHUNKS =
    """
      |select "SHARD_NAME", "SHARD_KEY_LOW", "SHARD_KEY_HIGH",
      |       "GROUP_KEY_LOW", "GROUP_KEY_HIGH",
      |       "CHUNK_ID", "GRP_ID", "CHUNK_UNIQUE_ID",
      |       "CHUNK_NAME", "PRIORITY", "STATE"
      |from "LOCAL_CHUNKS" c
      |where "TABFAM_ID" = ?
      |order by "GRP_ID", "CHUNK_ID"""".stripMargin


  def isShardedInstance(conn: Connection): Boolean = {
    try {
      performQuery(conn, IS_SHARDED_INSTANCE_QUERY) { rs =>
        rs.next()
      }
    } catch {
      case ex: SQLException => false
    }
  }

  def listShardInstances[V](dsKey: DataSourceKey)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_SHARD_INSTANCES, "list shard instances")(action)
  }

  def listReplicatedTables[V](dsKey: DataSourceKey)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_REPLICATED_TABLES, "list replicated tables")(action)
  }

  def listTableFamilies[V](dsKey: DataSourceKey)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_TABLE_FAMILIES, "list table families")(action)
  }

  def listTableFamilyColumns[V](dsKey: DataSourceKey, tFamId : Int)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_TABLE_FAMILY_COLUMNS,
      "list table family columns",
      ps => {
        ps.setInt(1, tFamId)
      }
    )(action)
  }

  def listTableFamilyChunks[V](dsKey: DataSourceKey, tFamId : Int)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_CHUNKS,
      "list table family chunks",
      ps => {
        ps.setInt(1, tFamId)
      }
    )(action)
  }
}
