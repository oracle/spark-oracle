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
package org.apache.spark.sql.oracle.tpcds

import java.io.{File, IOException, PrintWriter}
import java.util.Date

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LocalLimit, LocalRelation, LogicalPlan, Sort}
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.{OraSparkConfig, OraSparkUtils, SparkSessionExtensions}
import org.apache.spark.sql.types.{DataType, StringType, StructType}

/**
 * A tool for validating TPCDSQuery results
 * From IntelliJ you can run it with:
 * {{{
 *  Program Arguments: --queries q1,q2,q3 --db-instance scale1_tpcds --output-loc /Users/hbutani/newdb/spark-oracle/sql/src/test/resources/tpcds_validate
 *  Working directory: /Users/hbutani/newdb/spark-oracle/sql
 * }}}
 */
object TPCDSValidator {

  def main(args : Array[String]) : Unit = {
    val a = parseArgs(args)
    new TPCDSValidator(a).run
  }

  private case class Arguments(
                                dbInstance : String,
                                wallet_loc : Option[String],
                                outFolder : File,
                                queries : Seq[String],
                                rebuildResults : Boolean,
                                rebuildPlans : Boolean
                              ) {
    lazy val outPath = outFolder.getAbsolutePath

    def resultFileNm(qNm : String, isPushdown : Boolean) : String =
      s"${qNm}_${if (isPushdown) "pushdown" else "nonpushdown"}_res"

    def resultFilePath(qNm : String, isPushdown : Boolean) : String =
      s"${outPath}/${resultFileNm(qNm, isPushdown)}"

    def planFileNm(qNm : String, isPushdown : Boolean) : String =
      s"${qNm}_${if (isPushdown) "pushdown" else "nonpushdown"}_plan"

    def planFilePath(qNm : String, isPushdown : Boolean) : String =
      s"${outPath}/${planFileNm(qNm, isPushdown)}"

    def reportFileNm : String =
      s"validation_report"

    def reportFilePath : String =
      s"${outPath}/${reportFileNm}"
  }

  private case class ADiff(reason : String,
                           push : Option[String],
                           nonPush : Option[String],
                           isWarning : Boolean) {
    def dump(buf: StringBuilder): Unit = {
      buf.append(
        s"""  ${if (isWarning) "Warning: " else ""}${reason}
           |    push : ${push.getOrElse("<null>")}
           |    non-push : ${nonPush.getOrElse("<null>")}
           |""".stripMargin
      )
    }
  }

  private class ResultDiff(val qNm : String) {
    private val diffs : ArrayBuffer[ADiff] = ArrayBuffer[ADiff]()

    private var push_Result : () => String = null
    private var nonPush_Result : () => String = null

    def results(pushArr : Array[InternalRow],
                nonPushArr : Array[InternalRow]) : Unit = {
      push_Result = () => pushArr.mkString("\n")
      nonPush_Result = () => nonPushArr.mkString("\n")
    }

    def apply(s : String,
              pushVal : String,
              nonPushVal : String) : this.type = {
      apply(s, Some(pushVal), Some(nonPushVal), false)
    }

    def apply(s : String,
              pushVal : Option[String],
              nonPushVal : Option[String],
              isWarning : Boolean = false) : this.type = {
      diffs += ADiff(s, pushVal, nonPushVal, isWarning)
      this
    }

    def warning(s : String,
                pushVal : String,
                nonPushVal : String) : this.type = {
      apply(s, Some(pushVal), Some(nonPushVal), true)
    }

    def warning(s : String,
                pushVal : Option[String],
                nonPushVal : Option[String]) : this.type = {
      apply(s, pushVal, nonPushVal, true)
    }

    def isSame : Boolean = diffs.isEmpty

    def onlyWarnings : Boolean = diffs.forall(_.isWarning)

    def dump(buf: StringBuilder): Unit = {
      if (isSame) {
        buf.append(s"Query ${qNm} validation passed.")
      } else {
        if (onlyWarnings) {
          buf.append(s"Query ${qNm} validation passed with warnings:\n")
        } else {
          buf.append(s"Query ${qNm} validation failed:\n")
        }
        diffs.foreach(_.dump(buf))
      }
      buf.append("\n")
      buf.append("===========================================\n")
    }
  }

  private def printUsageAndExit(errCond : Option[String]) : Unit = {

    if (errCond.isDefined) {
      // scalastyle:off println
      System.err.println(errCond.get)
    }

    // scalastyle:off println
    System.err.println(
      s"""
         |Usage: TPCDSValidator [options]
         |Use to validate pushdown plans and results of tpcds queries.
         |Initially by specify --rebuild, can be used to build the resultsets in non-pushdown mode.
         |
         | Options are:
         |   --db-instance <a known instance such as mammoth_medium, scale1_tpcds>
         |   --wallet-loc <Location of wallet for oracle connections>
         |                for e.g. ~/Oracle/oce/work/wallet_MAMMOTH
         |   --output-loc <dir>
         |            Location where both pushdown and non-pushdown resylts and plans are stored.
         |   --queries <comma separated list fo tpcds query names such as q1, q2,...>
         |             If not specified all queries are evaluated.
         |   --rebuildQueries if specified non-pushdown query outputs are rebuild and stored.
         |   --rebuildPlans if specified pushdown query plans are rebuild and stored.
         |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }

  @throws[IOException]
  def mkdirs(directory: File) : Unit = {
    var i: Int = 0
    while (i < 5) {
      if (directory.exists) {
        if (directory.isDirectory) return
        throw new IOException("Could not create directory, because a file with the same " +
          "name already exists: " + directory.getAbsolutePath)
      }
      if (directory.mkdirs) return
      i += 1
    }
    throw new IOException("Could not create directory: " + directory.getAbsolutePath)
  }

  private def parseArgs(args: Array[String]) : Arguments = {
    var dbInstance : String = null
    var wallet_loc : Option[String] = None
    var outputFolderNm : String = null
    var queries : Seq[String] = TPCDSQueries.queries.map(_._1)
    var rebuildQueries : Boolean = false
    var rebuildPlans : Boolean = false

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--db-instance") :: value :: tail =>
          dbInstance = value
          argv = tail
        case ("--wallet-loc") :: value :: tail =>
          wallet_loc = Some(value)
          argv = tail
        case ("--output-loc") :: value :: tail =>
          outputFolderNm = value
          argv = tail
        case ("--queries") :: value :: tail =>
          queries = value.split(",").toSeq
          argv = tail
        case ("--rebuildQueries") :: tail =>
          rebuildQueries = true
          argv = tail
        case ("--rebuildPlans") :: tail =>
          rebuildPlans = true
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:on println
          printUsageAndExit(Some(s"Unrecognized options: ${tail.mkString(" ")}"))
      }
    }
    if (dbInstance == null) {
      printUsageAndExit(Some(s"--db-instance not specified"))
    }
    if (outputFolderNm == null) {
      printUsageAndExit(Some(s"--output-loc not specified"))
    }
    val outFolder = {
      val f = new File(outputFolderNm)
      mkdirs(f)
      f
    }

    Arguments(dbInstance, wallet_loc, outFolder, queries, rebuildQueries, rebuildPlans)
  }

  private val KNOWN_ISSUES : Map[String, String] = Map(
    "q67" ->
      """Oracle SQL: Creating a query block for order-by seems to change the resultset order.
        |So the following 2 return results in different orders even though their plans are the same.
        |The 2nd query returns the results in the wrong order.
        |
        |select i_category
        |     , sum(coalesce(ss_sales_price * ss_quantity, 0)) sumsales
        |from store_sales, item
        |where ss_item_sk = i_item_sk
        |group by rollup(i_category)
        |order by i_category ASC nulls first
        |
        |select i_category, sumsales
        |from
        |(
        |    select i_category
        |         , sum(coalesce(ss_sales_price * ss_quantity, 0)) sumsales
        |    from store_sales, item
        |    where ss_item_sk = i_item_sk
        |    group by rollup(i_category)
        |)
        |order by i_category ASC nulls first
        |;
        |""".stripMargin
  )

  private def dumpKnownIssues(buf : StringBuilder) : Unit = {
    buf.append(
      """KNOWN ISSUES:
        |-------------
        |""".stripMargin)
    for ((qNm, issue) <- KNOWN_ISSUES) {
      buf.append(s"Query ${qNm}:\n")
      buf.append(issue)
    }
    buf.append("===========================================\n")
  }

}

class TPCDSValidator(args: TPCDSValidator.Arguments) extends Logging {

  import org.apache.spark.sql.oracle.tpcds.TPCDSValidator._

  private def initializeSpark : Unit = {
    System.setProperty("spark.oracle.test.db_instance", args.dbInstance)
    if (args.wallet_loc.isDefined) {
      System.setProperty("spark.oracle.test.db_wallet_loc", args.wallet_loc.get)
    }

    new SparkSessionExtensions()(TestOracleHive.sparkSession.extensions)
    TestOracleHive.setConf("spark.sql.files.openCostInBytes", (128 * 1024 * 1024).toString)
    TestOracleHive.setConf("spark.sql.files.maxPartitionBytes", (16 * 1024 * 1024).toString)
    /*
     * enable querySplitting
     * max fetch tasks = 4 on a laptop with 8 task-slots(cores)
     */
    TestOracleHive.setConf("spark.sql.oracle.enable.querysplitting", "true")
    TestOracleHive.setConf("spark.sql.oracle.querysplit.maxfetch.rounds", "0.5")

    TestOracleHive.sql("use oracle")
  }

  private def writeToCSV(df: DataFrame, qNm: String, push : Boolean): Unit = {
    df.coalesce(1)
      .write
      .option("header", true)
      .option("sep", " | ")
      .mode("overwrite")
      .csv(s"${args.resultFilePath(qNm, push)}")
  }

  private def loadFromCSV(qNm: String, s : StructType): DataFrame = {
    TestOracleHive.sparkSession.
      read.
      schema(s).
      options(Map("delimiter" -> " | ", "header" -> "true")).
      csv(args.resultFilePath(qNm, false))
  }

  private def writeReport(buf : String) : Unit = {
    new PrintWriter(args.reportFilePath) {
      write(buf)
      close
    }
  }

  private def writePlan(qNm: String, logPlan : LogicalPlan): Unit = {
    new PrintWriter(args.planFilePath(qNm, true)) {
      write(logPlan.canonicalized.treeString)
      close
    }
  }

  private def loadPlan(qNm: String): String = {
    val source = scala.io.Source.fromFile(args.planFilePath(qNm, true))
    try source.mkString finally source.close()
  }

  private def executeAndSaveResult(qNm: String, sql : String, push : Boolean) : Unit = {
    try {
      OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, push)
      val df = TestOracleHive.sql(sql)
      writeToCSV(df, qNm, push)
    } finally {
      OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, true)
    }
  }

  private def outputReport(qDiffs : Seq[ResultDiff],
                           skippedQueries : Set[String]) : Unit = {
    val buf = new StringBuilder

    val totCount = qDiffs.size
    val passCount = qDiffs.filter(_.isSame).size
    val warningCount = qDiffs.filter(d => !d.isSame && d.onlyWarnings).size
    val failedQueries = qDiffs.filter(d => !d.isSame && !d.onlyWarnings)
    val failCount = failedQueries.size
    val failQNms = failedQueries.map(_.qNm)


    // scalastyle:off println
    buf.append(
      s"""Validation Report (generated on ${new Date()}):
        |  num_queries= (total=${totCount}, pass=${passCount}, warn=${warningCount}, fail=${failCount})
        |  skipped queries= ${skippedQueries.toList.sorted.mkString(",")}
        |  failed queries= ${failQNms.mkString(",")}
        |  queries with issues= ${KNOWN_ISSUES.keySet.mkString(",")}
        |
        |""".stripMargin
    )
    qDiffs.foreach(_.dump(buf))

    dumpKnownIssues(buf)

    writeReport(buf.toString())
    println(buf)
    // scalastyle:on println
  }

  private def roundValue(chooseRounding: Boolean, v: Any, dt: DataType): Any = {
    if (chooseRounding && v != null &&
      OraSparkUtils.isNumeric(dt) &&
      !Set[Any](
        Double.PositiveInfinity,
        Double.NegativeInfinity,
        Float.PositiveInfinity,
        Float.NegativeInfinity).contains(v)) {
      BigDecimal(v.toString).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else if (v == Float.PositiveInfinity) {
      Double.PositiveInfinity
    } else if (v == Float.NegativeInfinity) {
      Double.NegativeInfinity
    } else {
      v
    }
  }

  private def isSorted(nonPushPlan : LogicalPlan) : Boolean = {
    nonPushPlan match {
      case s : Sort => true
      case ll : LocalLimit => isSorted(ll.child)
      case gl : GlobalLimit => isSorted(gl.child)
      case _ => false
    }

  }

  private def validateResults(pushDF: DataFrame,
                              nonPushDF: DataFrame,
                              resultDiff : ResultDiff,
                              devAllowedInAproxNumeric: Double,
                              sorted: Boolean = false,
                              chooseRounding: Boolean = true
                               ): Unit = {

    if (pushDF.schema != nonPushDF.schema) {
      resultDiff.warning(
        s"Schema's are different",
        s"${pushDF.schema.mkString}",
        s"${nonPushDF.schema.mkString}"
      )
    }

    var df11 = pushDF
    var df21 = nonPushDF

    var df1_ilist = df11.queryExecution.executedPlan.executeCollect()
    var df2_ilist = df21.queryExecution.executedPlan.executeCollect()

    if (!sorted && df1_ilist.size > 1) {

      df1_ilist = {
        val sortCols1 = df11.columns
        df11 = OraSparkUtils.dataFrame(
          LocalRelation(df11.queryExecution.optimizedPlan.output, df1_ilist))(
          TestOracleHive.sparkSession.sqlContext)
        df11 =
          df11.sort(sortCols1.head, sortCols1.tail: _*).select(sortCols1.head, sortCols1.tail: _*)
        df11.queryExecution.executedPlan.executeCollect()
      }

      val sortCols2 = df21.columns

      df2_ilist = {
        df21 = OraSparkUtils.dataFrame(
          LocalRelation(df21.queryExecution.optimizedPlan.output, df2_ilist))(
          TestOracleHive.sparkSession.sqlContext)
        df21 =
          df21.sort(sortCols2.head, sortCols2.tail: _*).select(sortCols2.head, sortCols2.tail: _*)
        df21.queryExecution.executedPlan.executeCollect()
      }
    }

    resultDiff.results(df1_ilist, df2_ilist)

    val df1_count = df1_ilist.size
    val df2_count = df2_ilist.size
    if (df1_count != df2_count) {
      resultDiff(s"The row count is not equal",
        s"count=${df1_count}",
        s"count=${df2_count}"
      )
    } else {
      for (i <- 0 to df1_count.toInt - 1) {
        for (j <- 0 to pushDF.columns.size - 1) {
          val res1 = roundValue(
            chooseRounding,
            df1_ilist(i).get(j, df11.schema(j).dataType),
            df11.schema(j).dataType)
          val res2 = roundValue(
            chooseRounding,
            df2_ilist(i).get(j, df21.schema(j).dataType),
            df21.schema(j).dataType)
          // account for difference in null aggregation of javascript
          if (res2 == null && res1 != null) {
            if (!Set[Any](
              Int.MaxValue,
              Int.MinValue,
              Long.MaxValue,
              Long.MinValue,
              Double.PositiveInfinity,
              Double.NegativeInfinity,
              Float.PositiveInfinity,
              Float.NegativeInfinity,
              0).contains(res1)) {

              resultDiff(
                s"values in row $i, column $j don't match",
                s"value=${res1}",
                s"value=${res2}"
              )
            }
          } else if (!valuesEq(pushDF.schema(j).dataType, devAllowedInAproxNumeric, res1, res2)) {
            resultDiff(s"values in row $i, column $j don't match",
              s"push_value=${res1}",
              s"nonPushValue=${res2}"
            )
          }
        }
      }
    }
  }

  private def valuesEq(dt : DataType,
                       devAllowedInAproxNumeric: Double,
                       v1 : Any,
                       v2 : Any) : Boolean = {
    if (OraSparkUtils.isApproximateNumeric(dt)) {
      Math.abs(v1.asInstanceOf[Double] - v2.asInstanceOf[Double]) <= devAllowedInAproxNumeric
    } else if (v1 != null && v2 != null) {
      if (dt == StringType) {
        v1.toString.trim == v2.toString.trim
      } else {
        v1 == v2
      }
    } else {
      v1 == null && v2 == null
    }
  }

  private def validatePlan(qNm : String,
                           pushPlan : LogicalPlan,
                           diffResult : ResultDiff) : Unit = {
    val storedPlan = loadPlan(qNm)
    val currPlan = pushPlan.canonicalized.treeString
    if (storedPlan != pushPlan.canonicalized.treeString) {
      diffResult.warning(
        "Push Plan has Changed",
        Some(currPlan),
        None,
      )
    }
  }

  private def validateQuery(qNm : String) : ResultDiff = {

    val resultDiff = new ResultDiff(qNm)

    try {
      val sql = TPCDSQueryMap.queryMap(qNm)

      if (args.rebuildResults) {
        executeAndSaveResult(qNm, sql, false)

        // uncomment if you want the pushdown results
        // to compare with non-pushdown results.
        // DON'T checkin pushdown results.
        // executeAndSaveResult(qNm, sql, true)
      }

      val pushDF = TestOracleHive.sql(sql)
      val pushPlan = pushDF.queryExecution.optimizedPlan
      val nonPushDF = loadFromCSV(qNm, pushDF.schema)

      if (args.rebuildPlans) {
        writePlan(qNm, pushDF.queryExecution.optimizedPlan)
      } else {
        validatePlan(qNm, pushPlan, resultDiff)
      }

      validateResults(pushDF, nonPushDF, resultDiff, 0.0,
        isSorted(nonPushDF.queryExecution.optimizedPlan)
      )

      resultDiff
    } catch {
      case t : Throwable =>
        t.printStackTrace()
        resultDiff(
        "Internal Failure",
        Some(t.getMessage),
        None
      )
    }
  }

  // q14-1, q14-2, q23-2, q66
  val excludeSet : Set[String] = Set("q14-1", "q14-2", "q23-2", "q66")

  def run : Unit = {
    initializeSpark

    val results = for (qNm <- args.queries if !excludeSet.contains(qNm)) yield {
      validateQuery(qNm)
    }

    outputReport(results, excludeSet)
  }
}

