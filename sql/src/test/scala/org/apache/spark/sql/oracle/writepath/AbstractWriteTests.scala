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

package org.apache.spark.sql.oracle.writepath

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.oracle.OraMetadataMgrInternalTest
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, DeleteFromTableExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, V2CommandExec, V2TableWriteExec}
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.readpath.AbstractReadTests

abstract class AbstractWriteTests extends AbstractReadTests with OraMetadataMgrInternalTest {

  import AbstractWriteTests._

  /**
   * The test inserts involve reading from a spark_catalog.default namespace table
   * and writing to oracle.sparktest namespace. So insert statement has the form:
   * {{{insert into unit_test_write ... select .. from spark_catalog.default.src_tab_for_writes}}}
   *
   * But using the qualified name `spark_catalog.default.src_tab_for_writes` throws an exception
   * because [[TestHiveQueryExecution.analyzed]] invokes
   * [[MultipartIdentifierHelper.asTableIdentifier]] when collecting
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation]] from the plan.
   * This causes an exception to be thrown, as MultipartIdentifierHelper only
   * allows names with at most 2 parts.
   *
   * To get around this, we construct a [[QueryExecution]], bypassing
   * [[TestHiveQueryExecution.analyzed]] logic.
   *
   * @param sql
   * @return
   */
  def getAroundTestBugQE(sql: String): QueryExecution = {
    val plan = TestOracleHive.sparkSession.sessionState.sqlParser.parsePlan(sql)
    new QueryExecution(TestOracleHive.sparkSession, plan)
  }

  def getAroundTestBugDF(sql: String): DataFrame = {
    val qE = getAroundTestBugQE(sql)
    Dataset.ofRows(TestOracleHive.sparkSession, qE.analyzed)
  }

  def scenarioQE(scenario : WriteScenario) : Seq[QueryExecution] = scenario match {
    case cs : CompositeInsertScenario => cs.steps.map(scenarioQE).flatten
    case _ => Seq(getAroundTestBugQE(scenario.sql))
  }

  def scenarioDF(scenario : WriteScenario) : Seq[DataFrame] = scenario match {
    case cs : CompositeInsertScenario => cs.steps.map(scenarioDF).flatten
    case _ => Seq(getAroundTestBugDF(scenario.sql))
  }


  def scenarioInfo(scenario : WriteScenario) : (String, String) = {

    import scala.collection.JavaConverters._

    def appendDetails(op: AppendDataExec): String = {
      s"""
         |Append Operation:
         |  Destination table = ${op.table.name()}
         |  WriteOptions = ${op.writeOptions.asScala.mkString(",")}
         |Input query plan:
         |${op.query.treeString}
         |""".stripMargin
    }

    def overWrtByExprDetails(op: OverwriteByExpressionExec): String = {
      s"""
         |OverwriteByExpression Operation:
         |  Destination table = ${op.table.name()}
         |  Delete Filters = ${op.deleteWhere.mkString(", ")}
         |  WriteOptions = ${op.writeOptions.asScala.mkString(",")}
         |Input query plan:
         |${op.query.treeString}
         |""".stripMargin
    }

    def overWrtPartDynDetails(op: OverwritePartitionsDynamicExec): String = {
      s"""
         |OverwritePartitionsDynamic Operation:
         |  Destination table = ${op.table.name()}
         |  WriteOptions = ${op.writeOptions.asScala.mkString(",")}
         |Input query plan:
         |${op.query.treeString}
         |""".stripMargin
    }

    def deleteDetails(op : DeleteFromTableExec) : String = {
      s"""
         |DeleteFromTable Operation:
         |  Destination table = ${op.table.asInstanceOf[Table].name()}
         |  condition = ${op.condition.mkString(", ")}
         |  """.stripMargin
    }

    def writeOpInfo(sparkPlan : SparkPlan) : String = {
      val writeOps = sparkPlan find {
        case op : V2CommandExec => true
        case _ => false
      }

      if (writeOps.size == 0) {
        "No write operation in Spark Plan"
      } else if (writeOps.size > 1) {
        "multiple write operations in Spark Plan"
      } else {
        val writeOp = writeOps.head
        writeOp match {
          case op: AppendDataExec => appendDetails(op)
          case op: OverwriteByExpressionExec => overWrtByExprDetails(op)
          case op: OverwritePartitionsDynamicExec => overWrtPartDynDetails(op)
          case op : DeleteFromTableExec => deleteDetails(op)
          case _ => s"Unknown Operator Type: ${writeOp.getClass.getName}"
        }
      }
    }

    try {

      if (scenario.dynPartOvwrtMode) {
        TestOracleHive.sparkSession.sqlContext.setConf(
          "spark.sql.sources.partitionOverwriteMode",
          "dynamic"
        )
      }

      val r = for (qe <- scenarioQE(scenario)) yield {

        (s"""Scenario: ${scenario}
           |Write Operation details: ${writeOpInfo(qe.sparkPlan)}
           |""".stripMargin,
          s"""Scenario: ${scenario.name}
             |Logical Plan:
             |${qe.optimizedPlan}
             |Spark Plan:
             |${qe.sparkPlan}
             |""".stripMargin
          )
      }

      val (ops, plans) = r.unzip

      (ops.mkString("\n"), plans.mkString("\n"))

    } finally {
      TestOracleHive.sparkSession.sqlContext.setConf(
        "spark.sql.sources.partitionOverwriteMode",
        "static"
      )
    }

  }

  // scalastyle:off println
  def performTest(scenario : WriteScenario) : Unit = {
    test(scenario.name) {td =>
      println(s"Performing Write Scenario: ${scenario}")

      try {

        if (scenario.dynPartOvwrtMode) {
          TestOracleHive.sparkSession.sqlContext.setConf(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
          )
        }

        val (qEs, steps) = (scenarioQE(scenario), scenario.steps)

        for((qe, step) <- qEs.zip(steps)) {
          println(
            s"""Logical Plan:
               |${qe.optimizedPlan}
               |Spark Plan:
               |${qe.sparkPlan}""".stripMargin
          )
          scenarioDF(step)
        }

        scenario.validateTest

      } finally {
        TestOracleHive.sparkSession.sqlContext.setConf(
          "spark.sql.sources.partitionOverwriteMode",
          "static"
        )
        scenario.testCleanUp
      }
    }
  }
  // scalastyle:on println

  private def setupWriteSrcTab: Unit = {
    try {

      // Uncomment to create the src_tab_for_writes data file
      // AbstractWriteTests.write_src_df(1000)

      TestOracleHive.sql("use spark_catalog")

      val tblExists =
        TestOracleHive.sparkSession.sessionState.catalog.tableExists(TableIdentifier(src_tab))

      if (!tblExists) {

        TestOracleHive.sql(
          s"""
             |create table ${src_tab}(
             |${AbstractWriteTests.unit_test_table_cols_ddl}
             |)
             |using parquet
             |OPTIONS (path "${AbstractWriteTests.absoluteSrcDataPath}")
             |""".stripMargin).show()
      }
    } finally {
      TestOracleHive.sql("use oracle.sparktest")
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setupWriteSrcTab
  }
}

object AbstractWriteTests extends WriteTestsDataSetup with WriteScenarios {

  import org.scalacheck.Gen
  import org.scalacheck.Gen._

  val seed = org.scalacheck.rng.Seed(1234567)
  val params = Gen.Parameters.default.withInitialSeed(seed)

  val stateGen = oneOf("OR", "AZ", "PA", "MN", "NY", "CA", "OT")
  val channelGen = oneOf("D", "I", "U")

  lazy val state_channel_val: Iterator[(String, String)] = {

    listOfN(500, zip(stateGen, channelGen)).apply(params, seed).get.iterator
  }

  lazy val non_part_cond_values : Iterator[Boolean] = {
    // c_byte < 0 gen
    val gen = Gen.oneOf(true, false)
    listOfN(500, gen).apply(params, seed).get.iterator
  }

  val srcData_PartCounts = Map(
    "MN" -> 134,
    "AZ" -> 136,
    "PA" -> 136,
    "OT" -> 164,
    "OR" -> 144,
    "NY" -> 143,
    "CA" -> 143
  )

  val srcData_StateChannelCounts = Map(
    ("PA", "U") -> 42,
    ("OT", "U") -> 53,
    ("OT", "D") -> 49,
    ("AZ", "D") -> 58,
    ("MN", "U") -> 44,
    ("CA", "D") -> 43,
    ("OT", "I") -> 62,
    ("OR", "D") -> 41,
    ("AZ", "U") -> 41,
    ("MN", "D") -> 40,
    ("CA", "I") -> 51,
    ("PA", "D") -> 45,
    ("CA", "U") -> 49,
    ("OR", "U") -> 50,
    ("MN", "I") -> 50,
    ("PA", "I") -> 49,
    ("NY", "I") -> 45,
    ("AZ", "I") -> 37,
    ("NY", "U") -> 46,
    ("NY", "D") -> 52,
    ("OR", "I") -> 53
  )

  val src_tab = "src_tab_for_writes"
  val qual_src_tab = "spark_catalog.default.src_tab_for_writes"

  val scenarios : Seq[WriteScenario] = Seq(
    InsertScenario("insert non-part", false, false, false, false),
    InsertScenario("insert overwrite non-part", false, true, false, false),
    // partition table inserts
    InsertScenario("insert part", true, false, false, false),
    InsertScenario("insert part with partSpec", true, false, true, false),
    InsertScenario("insert overwrite part", true, true, false, false),
    InsertScenario("insert overwrite part in dynPartMode", true, true, false, true),
    InsertScenario("insert overwrite part with partSpec", true, true, true, false),
    InsertScenario("insert overwrite part with partSpec in dynPartMode", true, true, true, true)
  )

  val compositeScenarios = Seq(
    CompositeInsertScenario(
      "Non-Part multiple inserts",
      false,
      false,
      Seq(
        InsertScenario("insert non-part", false, false, false, false),
        InsertScenario("insert non-part", false, false, false, false)
      )
    ),
    CompositeInsertScenario(
      "Non-Part insert followed by insert overwrite",
      false,
      false,
      Seq(
        InsertScenario("insert non-part", false, false, false, false),
        InsertScenario("insert overwrite non-part", false, true, false, false)
      ),
      _ => 1000),
    CompositeInsertScenario(
      "Multiple part inserts",
      true,
      false,
      Seq(
        InsertScenario("insert part with partSpec", true, false, true, false),
        InsertScenario("insert part with partSpec", true, false, true, false),
      )
    ),
    CompositeInsertScenario(
      "Part insert followed by insert overwrite of part",
      true,
      false,
      Seq(
        InsertScenario("insert part with partSpec", true, false, true, false),
        InsertScenario("insert overwrite part with partSpec", true, true, true, false)
      ),
      steps => {
        val pVals0 = steps(0).asInstanceOf[InsertScenario].pvals
        val pVals1 = steps(1).asInstanceOf[InsertScenario].pvals

        if (pVals0._1 == pVals1._1) {
          steps(1).destTableSizeAfterScenario
        } else {
          steps.map(_.destTableSizeAfterScenario).sum
        }
      }
    ),
    CompositeInsertScenario(
      "Part insert in dynPart mode followed by insert overwrite in dynPart mode of part",
      true,
      false,
      Seq(
        InsertScenario("insert part with partSpec", true, false, true, false),
        InsertScenario("insert overwrite part with partSpec in dynPartMode", true, true, true, true)
      ),
      steps => {
        val pVals0 = steps(0).asInstanceOf[InsertScenario].pvals
        val pVals1 = steps(1).asInstanceOf[InsertScenario].pvals

        if (pVals0._1 == pVals1._1) {
          steps(1).destTableSizeAfterScenario
        } else {
          steps.map(_.destTableSizeAfterScenario).sum
        }
      }
    )
  )

  val deleteScenarios = Seq(
    CompositeInsertScenario(
      "Non-Part insert, followed by delete",
      false,
      false,
      Seq(
        InsertScenario("insert non-part", false, false, false, false),
        DeleteScenario(0, false)
      ),
      steps => steps.last.destTableSizeAfterScenario
    ),
    CompositeInsertScenario(
      "Part insert followed by delete of some part",
      true,
      false,
      Seq(
        InsertScenario("insert part with partSpec", true, false, true, false),
        DeleteScenario(5, true)
      ),
      steps => {
        val pVals0 = steps(0).asInstanceOf[InsertScenario].pvals
        val pVals1 = steps(1).asInstanceOf[DeleteScenario].pvals

        if (pVals0._1 == pVals1._1) {
          steps(1).destTableSizeAfterScenario
        } else {
          steps(0).destTableSizeAfterScenario
        }
      }
    ),
    CompositeInsertScenario(
      "Part insert followed by delete of same part",
      true,
      false, {
        val iScn = InsertScenario("insert part with partSpec", true, false, true, false)
        Seq(
          iScn,
          DeleteScenario(5, true, Some(iScn.pvals))
        )
      },
      steps => {
        val delScn = steps(1).asInstanceOf[DeleteScenario]
        steps(0).asInstanceOf[InsertScenario].destTableSizeAfterScenario -
          srcData_StateChannelCounts(delScn.pvals)
      }
    )
  )


}
