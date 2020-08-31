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
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf

object OraSparkConfig {

  val VARCHAR2_MAX_LENGTH =
    buildConf("spark.sql.oracle.max_string_size")
      .doc("Used as the varchar2 datatype length when translating string datatype. " +
        "Default is 4000")
      .intConf
      .createWithDefault(4000)

  val ENABLE_ORA_PUSHDOWN = buildConf(
    "spark.sql.oracle.enable.pushdown").
    doc("Enable Pushdown of Spark Operators as Oracle Queries").
    booleanConf.createWithDefault(true)

  val ENABLE_ORA_QUERY_SPLITTING = buildConf(
    "spark.sql.oracle.enable.querysplitting").
    doc("""Enable Splitting Oracle Pushdown Queries into multiple
    |queries, one per task. Currently default is false.
    |The process of inferring Query splits, runs an explain on
    |the pushdown query. This may incur an overhead of 100s of mSecs.
    |In situations where it is ok to always run 1 task or
    |when explain overhead is high, set to false.
    |Typically low latency queries return small amounts of data
    |so setting up a single task is reasonable.
    |In the future we will provide a mechanism for a
    |user to specify split strategy""".stripMargin
    ).
    booleanConf.
    createWithDefault(true)

  val BYTES_PER_SPLIT_TASK =
    buildConf("spark.sql.oracle.querysplit.target")
      .doc(
        """Split pushdown query so that each Task returns these many bytes
          |(specified in MB). Default is 1MB.
          |
          |But number of fetch tasks upper bounded by
          | `spark.sql.oracle.querysplit.maxfetch.rounds`""".stripMargin)
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(JavaUtils.byteStringAs("1MB", ByteUnit.BYTE))

  val MAX_SPLIT_FETCH_TASKS =
    buildConf("spark.sql.oracle.querysplit.maxfetch.rounds")
      .doc(
        """We will split the fetching into tasks based on the
          |`spark.sql.oracle.querysplit.target`, but the maximum number of fetch tasks
          |will be Math.floor(`this_value` * default_parallelism) of the cluster.
          |
          |So suppose `spark.sql.oracle.querysplit.target`=1MB; the fetch size is 100MB, the
          |defaultParallelism=8, and this_value is 1. That means even though according to the
          |querysplit.target setting we should have 100 tasks; we will be restricted to
          | 8 tasks(= 8 * 1)
          |""".stripMargin)
      .doubleConf
      .createWithDefault(1.0)

  val ALLOW_SPLITBY_RESULTSET = buildConf(
    "spark.sql.oracle.allow.splitresultset").
    doc(
      """Many queries that cannot be split by rows/partitions
        |of one of the tables scanned in  the query.
        |Most such queries will return small amout of data, so no
        |splitting is needed. But when a large amount of data
        |is returned by such a query, a possible splitting strategy
        |is to split by resultset using ` OFFSET ? ROWS FETCH NEXT ? ROWS ONLY` clause.
        |But query tasks issue the same query with different fetch batches,
        |potentially of different jdbc connections may strain the
        |server. So we allow for result based splitting to be turned off
        |By default it is turned on.""".stripMargin
    ).
    booleanConf.
    createWithDefault(true)

  val TABLESPACE_FOR_TEMP_TABLES = buildConf(
    "spark.sql.oracle.temp_table.tablespace").
    doc(
      """For DML operations pushed down from Spark, new rows are first written to a temp table.
        |At job commit time the destination table is updated.
        |We cannot create the temp table as an Oracle temp table because
        |the data needs to accessible from many sessions: connections in the executor slot write rows
        |and a connection the driver moves data from the temp table to the destination table.
        |Use this paramater to specify what tablespace should be used to store this temp data.
        |By default this is not set, and we store the temp data in the default tablespace of
        |the connection.
        |""".stripMargin
    ).stringConf.createOptional


  val INSERT_INTO_DEST_TABLE_STAT_HINTS = buildConf(
    "spark.sql.oracle.insert_into_dest_table.hints").
    doc(
      """When moving rows from the temporary table to the destination table
        |the operation can be optimized by specifying hints on the INSERT:
        |(See https://docs.oracle.com/en/database/oracle/oracle-database/21/vldbg/parallel-exec-tips.html#GUID-A4227A4C-209A-40B9-9A68-A57803E66C04)
        |- APPEND hint for doing a direct insert
        |- PARALLEL hint to trigger parallelizing insert operation.
        |- NOLOGGING hint to avoid redo-undo log overhead.
        |
        |We cannot automate these choices. So users can specify the hints to be applied for a particular DML operation.
        |
        |Why cannot we automate these choices?
        |- NOLOGGING is an operational choice; so we cannot decide this.
        |- We know whether Insert is an APPEND or OVERWRITE; but since Oracle supports RANGE and INTERVAL partitioning
        |  an OVERWRITE doesn't mean a reconstruction of a partition.
        |- Currently there is a single write flow for all use cases.
        |  In the future we plan to provide a special flow for List partitions. In that case we can infer
        |  APPEND for the overwrite case.
        |- See link above; PARALLEL hint on its own either has no effect or will imply APPEND insert mode.
        |  We cannot make this decision; because APPEND mode implies not using free space in existing blocks.
        |  This may cause space wasteage that the user is not willing to accept.
        |
        |""".stripMargin
    ).stringConf.createOptional

  // config entries for sharding

  val ENABLE_SHARDING_PUSHDOWN = buildConf(
    "spark.sql.oracle.enable.sharding_pushdown").
    doc(
      """If this is true then we will attempt to rewrite pushdown queries
        |targetted for the Shard Coordinator, into plans that directly evaluate sub-plans
        |on shard instances and do the merge/coordination steps originally done in the Shard
        | coordinator as Spark Operators.
        |
        |We are actually undoing the pushdown of the original Spark logical Plan
        |to a point where any pushed sub-plan can be executed directly on shard instances.
        |
        |For example consider Aggregate(Orders join LineItem, GBy Status, Sum of Price)
        |on the TPCH schema sharded by Order_Key.
        |A full pushdown would pushd ths as an Oracle SQL to the Shard coordinator.
        |But this may require a lot of coordination and work on the Coordinator.
        |With this flag true we may convert this into the Plan:
        |Spark_Agg(OraclePlan(Orders join LineItem),GBy Status, Sum of Price)
        |Now the join can be done independently by Shard Instances and
        |read into Spark executors. This plan potentially has lower latency and
        |avoids overloading the Shard coordinator.""".stripMargin).
    booleanConf.createWithDefault(true)

  val SHARDING_PUSHDOWN_MODE = buildConf("spark.sql.oracle.sharding_pushdown.mode")
    .doc(
      """The rewrite decision of a query targetted to the Coordinator is
        |heuristic based. Either it is based on the estimated cost of execution
        |in the Coordinator or on the existence of certain operators in the Coordinator plan.
        |This value can be 'cost' or 'operator'
        |
        |Cost based pushdown will trigger a sharding pushdown if the estimate for executing
        | in the coordinator exceeds the threshold value
        | 'spark.sql.oracle.sharding_pushdown.cost.threshold'
        |
        |Operator based pushdown will trigger if the coordinator plan contains table accesses
        |or joins. The plan will be rewritten in their presence even if the cost estimate is
        |below the 'spark.sql.oracle.sharding_pushdown.cost.threshold' value.
        | """.stripMargin)
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValue(mode => Set("COST", "OPERATOR").contains(mode),
      "Invalid value for 'spark.sql.oracle.sharding_pushdown.mode'." +
        " Valid values are 'cost' and 'operator'")
    .createWithDefault("cost")

  val SHARDING_PUSHDOWN_COST_THRESHOLD = buildConf(
    "spark.sql.oracle.sharding_pushdown.cost.threshold")
    .doc(
      """In cost sharding_pushdown mode any coordinator plan that is estimated to
        |take more than this value is considered a candidate for rewrite.
        | """.stripMargin)
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(1L)



  // finish config entries for sharding


  def getConf[T](configEntry : ConfigEntry[T])(
    implicit sparkSession : SparkSession = OraSparkUtils.currentSparkSession
  ) : T = {
    getConf(configEntry, sparkSession.sqlContext.conf)
  }

  def getConf[T](configEntry : ConfigEntry[T],
                 conf: SQLConf) : T = {
    conf.getConf(configEntry)
  }

  def setConf[T](configEntry : ConfigEntry[T], value : T)(
    implicit sparkSession : SparkSession = OraSparkUtils.currentSparkSession
  ) : Unit = {
    setConf(configEntry, value, sparkSession.sqlContext.conf)
  }

  def setConf[T](configEntry : ConfigEntry[T],
                 value : T,
                 conf: SQLConf) : Unit = {
    conf.setConf(configEntry, value)
  }
}
