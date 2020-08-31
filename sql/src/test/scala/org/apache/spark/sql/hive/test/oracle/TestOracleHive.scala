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

package org.apache.spark.sql.hive.test.oracle

import java.util.Properties

import scala.collection.mutable.{Map => MMap}
import scala.io.Source

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH

object OracleTestConf {

  private lazy val commonConf = new SparkConf()
    .set("spark.sql.test", "")
    .set(SQLConf.CODEGEN_FALLBACK.key, "false")
    .set(
      HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
      "org.apache.spark.sql.hive.execution.PairSerDe")
    .set(WAREHOUSE_PATH.key, TestHiveContext.makeWarehouseDir().toURI.getPath)
    // SPARK-8910
    .set(UI_ENABLED, false)
    .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
    // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
    // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
    .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
    // Disable ConvertToLocalRelation for better test coverage. Test cases built on
    // LocalRelation will exercise the optimization rules better by disabling it as
    // this rule may potentially block testing of other optimization rules such as
    // ConstantPropagation etc.
    .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
    .set("spark.sql.extensions", "org.apache.spark.sql.oracle.SparkSessionExtensions")
    .set(
      "spark.kryo.registrator",
      "org.apache.spark.sql.connector.catalog.oracle.OraKryoRegistrator")
    /*
    Uncomment to see Plan rewrites
    .set("spark.sql.planChangeLog.level", "ERROR")
    .set(
      "spark.sql.planChangeLog.rules",
      "org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions," +
        "org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown," +
        "org.apache.spark.sql.catalyst.optimizer.RewritePredicateSubquery," +
        "org.apache.spark.sql.catalyst.optimizer.PullupCorrelatedPredicates")
     */
    .set(
      "spark.sql.catalog.oracle",
      "org.apache.spark.sql.connector.catalog.oracle.OracleCatalog")
    .set("spark.sql.catalog.oracle.use_metadata_cache_only", "true")
    .set("spark.sql.catalog.oracle.metadata_cache_loc", "src/test/resources/metadata_cache")
    .set("spark.sql.catalog.oracle.log_and_time_sql.enabled", "true")
    .set("spark.sql.catalog.oracle.log_and_time_sql.log_level", "info")
    .set("spark.sql.catalog.oracle.fetchSize", "100000")
    .set("spark.sql.oracle.enable.querysplitting", "false")
    // .set("spark.sql.oracle.max_string_size", "32767")
  /* Use these settings to turn off some of the code generation
    .set("spark.sql.codegen.factoryMode", "NO_CODEGEN")
    .set("spark.sql.codegen.maxFields", "0")
    .set("spark.sql.codegen.wholeStage", "false")
   */

  lazy val localConf: SparkConf = {

    var conf = commonConf

    assert(
      System.getProperty(SPARK_ORACLE_DB_INSTANCE) != null,
      s"Running test requires setting ${SPARK_ORACLE_DB_INSTANCE} system property")

    System.getProperty(SPARK_ORACLE_DB_INSTANCE) match {
      case "local_hb" =>
        conf = local_hb(conf)
      case "local_sc" =>
        conf = local_sc(conf)
      case "local_tpcds" =>
        conf = local_tpcds(conf)
      case "mammoth_medium" =>
        assert(
          System.getProperty(SPARK_ORACLE_DB_WALLET_LOC) != null,
          s"Use of mammoth instance requires setting ${SPARK_ORACLE_DB_WALLET_LOC} system property")
        conf = mammoth_medium(conf)
      case "scale1_tpcds" =>
        conf = scale1_tpcds(conf)
      case "sharding" =>
        conf = sharding(conf)
      // scalastyle:off
      case i => setOraInstanceProps(i, conf)
      // scalastyle:on
    }
    conf
  }

  /*
   * Ensure you have spark-ora-test.properties in your classpath
   * Its entries should be of the form
   * <instance_alias>.<spark-property>=value
   *
   * for example:
   * local_hb.spark.sql.catalog.oracle.user=sh
   */

  lazy val sparkOraTestProperties : MMap[String, String] = {
    val url = getClass.getResource("/spark-ora-test.properties")
    val source = Source.fromURL(url)

    val properties = new Properties()
    properties.load(source.bufferedReader())
    import scala.collection.JavaConverters._
    properties.asScala
  }

  private def setOraInstanceProps(prefix : String,
                                  conf: SparkConf) : SparkConf = {
    for ((k, v) <- sparkOraTestProperties if k.startsWith(prefix) ) {
      val key = k.substring(prefix.size + 1)
      conf.set(key, v)
    }
    conf
  }

  def local_hb(conf: SparkConf): SparkConf = setOraInstanceProps("local_hb", conf)

  def local_sc(conf: SparkConf): SparkConf = setOraInstanceProps("local_sc", conf)

  def local_tpcds(conf: SparkConf): SparkConf = setOraInstanceProps("local_tpcds", conf)

  def mammoth_medium(conf: SparkConf): SparkConf = setOraInstanceProps("mammoth_medium", conf)

  def scale1_tpcds(conf: SparkConf): SparkConf = setOraInstanceProps("scale1_tpcds", conf)

  def sharding(conf: SparkConf): SparkConf = setOraInstanceProps("sharding", conf)

  def testMaster: String = "local[*]"

  val SPARK_ORACLE_DB_INSTANCE = "spark.oracle.test.db_instance"
  val SPARK_ORACLE_DB_WALLET_LOC = "spark.oracle.test.db_wallet_loc"

  // probability of running a long test
  val SPARK_ORACLE_RUN_LONG_TEST_PROB = "spark.oracle.test.long_test.run.prob"

  lazy val longTestRunProb : Double = {
    System.getProperty(SPARK_ORACLE_RUN_LONG_TEST_PROB) match {
      case x if x != null => scala.util.Try(x.toDouble).getOrElse(1.0)
      case _ => 1.0
    }
  }

  lazy val r = new scala.util.Random

  def runLongTest(prob : Double = longTestRunProb) : Boolean = {
    r.nextDouble() < prob
  }

}

object TestOracleHive
    extends TestHiveContext(
      new SparkContext(
        System.getProperty("spark.sql.test.master", OracleTestConf.testMaster),
        "TestSQLContext",
        OracleTestConf.localConf),
      false)
