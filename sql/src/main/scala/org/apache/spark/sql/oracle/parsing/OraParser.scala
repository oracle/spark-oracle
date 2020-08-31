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
package org.apache.spark.sql.oracle.parsing

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedTable}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.oracle.{OracleCatalog, OracleTable}
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.oracle.commands.{ExplainPushdown, OraShowPartitions, SparkOraVersion}
import org.apache.spark.sql.oracle.rules.OraLogicalRules
import org.apache.spark.sql.types.{DataType, StructType}

class OraParser(baseParser : ParserInterface) extends ParserInterface {

  /**
   * Ensure that the [[OraSQLPushdownRule]] is
   * applied as an `extraOptimizations`. This ensures that the
   * [[OraSQLPushdownRule]] is applied in the *User Provided Optimizers*
   * **Batch** of the [[org.apache.spark.sql.execution.SparkOptimizer]],
   * which comes after all built-in rewrites. The rewrites in
   * [[OraSQLPushdownRule]] assume the input [[LogicalPlan]] has been
   * transformed by all Spark rewrite rules.
   * @param ss
   */
  private def ensurePushdownRuleRegistered(ss : SparkSession) : Unit = {
    val extraRules = ss.sessionState.experimentalMethods.extraOptimizations

    if (!extraRules.contains(OraLogicalRules)) {
      ss.sessionState.experimentalMethods.extraOptimizations =
        ss.sessionState.experimentalMethods.extraOptimizations :+ OraLogicalRules
    }
  }

  val sparkOraExtensionsParser = new SparkOraExtensionsParser(baseParser)

  override def parsePlan(sqlText: String): LogicalPlan = {
    ensurePushdownRuleRegistered(OraSparkUtils.currentSparkSession)

    val extensionsPlan = sparkOraExtensionsParser.parseExtensions(sqlText)

    if (extensionsPlan.successful ) {
      extensionsPlan.get
    } else {
      try {
        baseParser.parsePlan(sqlText)
      } catch {
        case pe : ParseException =>
          val splFailureDetails =
            extensionsPlan.asInstanceOf[SparkOraExtensionsParser#NoSuccess].msg
          throw new ParseException(pe.command,
            s"""${pe.message}
               |[
               |Also failed to parse as spark-ora extensions:
               | ${splFailureDetails}
               |]""".stripMargin,
            pe.start,
            pe.stop
          )
      }
    }
  }

  def parseExpression(sqlText: String): Expression =
    baseParser.parseExpression(sqlText)

  def parseTableIdentifier(sqlText: String): TableIdentifier =
    baseParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    baseParser.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
  baseParser.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    baseParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    baseParser.parseDataType(sqlText)

}


private[parsing] class SparkOraExtensionsParser(val baseParser : ParserInterface)
  extends AbstractLightWeightSQLParser {

  private def sparkSession = SparkSession.getActiveSession.get

  protected val SPARK_ORA = Keyword("SPARK_ORA")
  protected val VERSION = Keyword("VERSION")
  protected val EXPLAIN = Keyword("EXPLAIN")
  protected val ORACLE = Keyword("ORACLE")
  protected val PUSHDOWN = Keyword("PUSHDOWN")
  protected val SHOW = Keyword("SHOW")
  protected val PARTITIONS = Keyword("PARTITIONS")

  protected val SHARD_INSTANCES = Keyword("SHARD_INSTANCES")
  protected val TABLE_FAMILIES = Keyword("TABLE_FAMILIES")
  protected val REPLICATED = Keyword("REPLICATED")
  protected val SHARDED = Keyword("SHARDED")
  protected val TABLES = Keyword("TABLES")
  protected val ROUTING_TABLE = Keyword("ROUTING_TABLE")
  protected val SHARD_EXECUTOR_AFFINITY = Keyword("SHARD_EXECUTOR_AFFINITY")
  protected val ORACLE_CATALOG = Keyword("ORACLE_CATALOG")

  def parseExtensions(input: String): ParseResult[LogicalPlan] = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input))
  }

  override protected def start: Parser[LogicalPlan] =
    sparkOraVersion | explainPushdown | oracleShowPartitions |
      showShardInstances | showTableFamilies |
      showReplicatedTables | showShardedTables |
      showRoutingTable


  private lazy val sparkOraVersion : Parser[LogicalPlan] =
    SPARK_ORA ~ VERSION ^^ {
      case _ => SparkOraVersion()
    }

  private lazy val explainPushdown : Parser[LogicalPlan] =
    (EXPLAIN ~ ORACLE ~ PUSHDOWN) ~> restInput ^^ {
      case sqlText =>
        val df = sparkSession.sql(sqlText)
        ExplainPushdown(df.queryExecution.sparkPlan)
    }

  private lazy val oracleShowPartitions : Parser[LogicalPlan] =
    (SHOW ~ ORACLE ~ PARTITIONS) ~> multiPartId ^^ {
      case mId =>
        createCommand(mId, "show oracle partitions", oraTab => OraShowPartitions(oraTab))
    }

  private lazy val showShardInstances : Parser[LogicalPlan] =
    (SHOW ~ SHARD_INSTANCES) ^^ {
      case _ => ListShardInstances
    }

  private lazy val showTableFamilies : Parser[LogicalPlan] =
    (SHOW ~ TABLE_FAMILIES) ^^ {
      case _ => ListTableFamilies
    }

  private lazy val showReplicatedTables : Parser[LogicalPlan] =
    (SHOW ~ REPLICATED ~ TABLES) ^^ {
      case _ => ListReplicatedTables
    }

  private lazy val showShardedTables : Parser[LogicalPlan] =
    (SHOW ~ SHARDED ~ TABLES) ^^ {
      case _ => ListShardedTables
    }

  private lazy val showRoutingTable : Parser[LogicalPlan] =
    (SHOW ~ ROUTING_TABLE) ~> multiPartId ^^ {
      case mId => createCommand(mId, "show routing table", oraTab => ListRoutingTable(oraTab))
    }

  private lazy val qualifiedId : Parser[Seq[String]] =
    (ident ~ ("." ~> ident).?) ^^ {
      case ~(n, None) => Seq(n)
      case ~(q, Some(n)) => Seq(q, n)
    }

  private lazy val multiPartId : Parser[Seq[String]] =
    ((ident | ORACLE) ~ ("." ~> qualifiedId).?) ^^ {
      case ~(n, None) => Seq(n)
      case ~(q, Some(s)) => q +: s
    }

  private def createCommand(mId: Seq[String],
                           cmdString : String,
                           createCmd : OracleTable => LogicalPlan) : LogicalPlan = {
    val tab = UnresolvedTable(mId, cmdString)
    val resolvedTab = sparkSession.sessionState.analyzer.execute(tab)
    if (resolvedTab.isInstanceOf[ResolvedTable]) {
      val resovTab = resolvedTab.asInstanceOf[ResolvedTable]
      if (resovTab.catalog.isInstanceOf[OracleCatalog]) {
        createCmd(resovTab.table.asInstanceOf[OracleTable])
      } else {
        throw new AnalysisException(s"Cannot run ${cmdString}" +
          s" command on a non-oracle table  ${resovTab.identifier}")
      }
    } else {
      throw new AnalysisException(
        s"Cannot run ${cmdString} command: failed to resolve table ${mId.mkString(".")}"
      )
    }
  }
}