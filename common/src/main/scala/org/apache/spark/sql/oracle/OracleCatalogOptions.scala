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

package org.apache.spark.sql.oracle

import java.util.Locale

import scala.language.implicitConversions
import scala.util.Try

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 *
 * @param maxPartitions
 * @param partitionerType
 * @param fetchSize
 * @param isChunkSplitter
 * @param chunkSQL
 * @param metadata_cache_only should [[OracleMetadataManager]] only serve requests
 *                            from contents of the cache
 * @param metadataCacheLoc location of Metadata cache; if not specifed a temporary folder is used.
 * @param oci_credential_name
 * @param logSQLBehavior
 */
case class OracleCatalogOptions(
    maxPartitions: Int,
    partitionerType: String,
    fetchSize: Int,
    isChunkSplitter: Boolean,
    chunkSQL: Option[String],
    metadata_cache_only : Boolean,
    metadataCacheLoc: Option[String],
    oci_credential_name: Option[String],
    logSQLBehavior: LoggingAndTimingSQL)

object OracleCatalogOptions {

  private val catalogOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    catalogOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val ORACLE_JDBC_MAX_PARTITIONS = newOption("maxPartitions")
  val ORACLE_JDBC_PARTITIONER_TYPE = newOption("partitionerType")
  val ORACLE_FETCH_SIZE = newOption("fetchSize")
  val ORACLE_JDBC_IS_CHUNK_SPLITTER = newOption("isChunkSplitter")
  val ORACLE_CUSTOM_CHUNK_SQL = newOption("customPartitionSQL")
  val ORACLE_PARALLELISM = newOption("useOracleParallelism")
  val ORACLE_USE_METADATA_CACHE_ONLY = newOption("use_metadata_cache_only")
  val ORACLE_METADATA_CACHE = newOption("metadata_cache_loc")
  val ORACLE_OCI_CREDENTIAL_NAME = newOption("oci_credential_name")

  val DEFAULT_MAX_SPLITS = 1

  def catalogOptions(parameters: CaseInsensitiveMap[String]): OracleCatalogOptions = {

    OracleCatalogOptions(
      parameters.getOrElse(ORACLE_JDBC_MAX_PARTITIONS, "1").toInt,
      parameters.getOrElse(ORACLE_JDBC_PARTITIONER_TYPE, "SINGLE_SPLITTER"),
      parameters.getOrElse(ORACLE_FETCH_SIZE, "10").asInstanceOf[String].toInt,
      parameters.getOrElse(ORACLE_JDBC_IS_CHUNK_SPLITTER, "true").toBoolean,
      parameters.get(ORACLE_CUSTOM_CHUNK_SQL),
      parameters.get(ORACLE_USE_METADATA_CACHE_ONLY).map(_.toBoolean).getOrElse(false),
      parameters.get(ORACLE_METADATA_CACHE),
      parameters.get(ORACLE_OCI_CREDENTIAL_NAME),
      LoggingAndTimingSQL.fromOptions(parameters))
  }

}

/**
 * based on [[LoggingSQLAndTimeSettings]] from scalikejdbc library.
 */
case class LoggingAndTimingSQL(
    enabled: Boolean,
    logLevel: Int,
    stackTraceDepth: Int,
    enableSlowSqlWarn: Boolean,
    slowSqlThreshold: Long,
    slowSqlLogLevel: Int)

object LoggingAndTimingSQL {

  object LogLevel extends Enumeration {
    val INFO, DEBUG, WARNING, ERROR, TRACE = Value

    implicit def fromString(s: String): LogLevel.Value =
      Try {
        withName(s.toUpperCase(Locale.ROOT))
      }.getOrElse(WARNING)
  }

  private val optionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    optionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val ENABLED = newOption("log_and_time_sql.enabled")
  val LOG_LEVEL = newOption("log_and_time_sql.log_level")
  val STACK_TRACE_DEPTH = newOption("log_and_time_sql.stacktrace_depth")
  val ENABLE_SLOWSQL_WARNING = newOption("log_and_time_sql.warn_slow_sql")
  val SLOWSQL_THRESHOLD = newOption("log_and_time_sql.slow_sql_threshold_millis")
  val SLOWSQL_LOGLEVEL = newOption("log_and_time_sql.slow_sql_log_level")

  def fromOptions(parameters: CaseInsensitiveMap[String]): LoggingAndTimingSQL = {
    LoggingAndTimingSQL(
      parameters.getOrElse(ENABLED, "false").toBoolean,
      LogLevel.fromString(parameters.getOrElse(LOG_LEVEL, "WARNING")).id,
      parameters.getOrElse(STACK_TRACE_DEPTH, "10").asInstanceOf[String].toInt,
      parameters.getOrElse(ENABLE_SLOWSQL_WARNING, "true").toBoolean,
      parameters.getOrElse(SLOWSQL_THRESHOLD, "10000").toLong,
      LogLevel.fromString(parameters.getOrElse(LOG_LEVEL, "WARNING")).id)
  }
}
