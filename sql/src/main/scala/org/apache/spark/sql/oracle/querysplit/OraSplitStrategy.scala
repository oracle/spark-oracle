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
package org.apache.spark.sql.oracle.querysplit

import oracle.spark.{DataSourceKey, ORASQLUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.oracle.OraUnknownDistribution
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.oracle.{OraSparkConfig, OraSparkUtils, OraSQLImplicits, SQLSnippet}
import org.apache.spark.sql.oracle.expressions.{OraExpressions, OraLiteralSql}
import org.apache.spark.sql.oracle.operators.{OraPlan, OraTableScan}
import org.apache.spark.sql.oracle.util.TimeIt

trait OraSplitStrategy {

  val splitList : IndexedSeq[OraDBSplit]

  def splitIds : Range = (0 until splitList.size)

  /**
   * Based on how data is distributed among the [[OraDBSplit]]s
   * we can associate a [[Partitioning]] to the [[OraScan]]
   * that this Strategy applie to.
   * @return
   */
  def partitioning: Partitioning = OraUnknownDistribution(0)

  /**
   * A [[OraDBSplit]] potentially implies a block or partition restriction.
   * In the future this information can be used to come up with `preferred locations`
   * for a split.
   * - some way to associate block/partitions to physical hosts
   * - some environment information that associates spark nodes with oracle nodes
   *
   * @param splitId
   * @return
   */
  def preferredLocs(splitId : Int): Array[String] = Array.empty

  /**
   * [[OraDBSplit]] implies an optional rowid or partition range
   * to be applied to an Oracle Table. In an [[OraPlan]] this is
   * applied by replacing a [[OraTableScan]] reference with a
   * query block that selects the necessary table columns with the
   * implied row/partition range in the where/partition clause.
   *
   * @param oraTblScan
   * @param splitId
   * @return
   */
  def splitOraSQL(oraTblScan : OraTableScan, splitId : Int) : Option[SQLSnippet]

  /**
   * Instance where this {{{splitId}}} should be executed.
   * For non-shard instances this is the same as the {{{planningDSKey}}}.
   * For shard instances for [[ShardQuery ShardQueries]] this would be a particular
   * shard instance.
   *
   * @param planningDSKey
   * @param splitId
   * @return
   */
  def dskey(planningDSKey : DataSourceKey,
            splitId : Int
           ) : DataSourceKey = planningDSKey

  protected def tableSelectList(oraTableScan: OraTableScan) : Seq[SQLSnippet] = {
    OraExpressions.unapplySeq(oraTableScan.catalystAttributes).get
      .map(_.reifyLiterals.orasql)
  }

  protected def tableFrom(ot: OraTableScan) : SQLSnippet =
    SQLSnippet.tableQualId(ot.oraTable)

  protected def isTarget(oraTblScan: OraTableScan,
                         targetTable : TableAccessDetails
                        ) : Boolean = {
    oraTblScan.isQuerySplitCandidate &&
      oraTblScan.oraTable.schema == targetTable.oraTable.schema &&
      oraTblScan.oraTable.name == targetTable.oraTable.name
  }

  /**
   * [[OraResultSplitStrategy]] can use this hook to add a fetch clause:
   * `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`
   *
   * Each fetch batch is run as a separate query. To ensure all
   * invocations have the same overall resultset, add an order by
   * clause of original query doesn't have one.
   *
   * TODO: what about data consistency across query invocations.
   * Run all queries of a [[https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SELECT.html#GUID-CFA006CA-6FF1-4972-821E-6996142A51C6 specific SCN]]
   * Each table reference would need to have to be associated `flashback_query_clause`.
   *
   * @param sqlSnip
   * @param splitId
   * @return
   */
  def associateFetchClause(sqlSnip: SQLSnippet,
                           addOrderBy : Boolean,
                           orderByCnt : Int,
                           splitId : Int) : SQLSnippet = sqlSnip

  protected def literalsql(v : Any) : OraLiteralSql =
    new OraLiteralSql(v.toString)

  def explain(append: String => Unit): Unit

}

case object NoSplitStrategy extends OraSplitStrategy {
  val splitList = IndexedSeq(OraNoSplit)

  override def splitOraSQL(oraTblScan: OraTableScan, splitId : Int): Option[SQLSnippet] =
    None

  def explain(append: String => Unit): Unit =
    append("Query is not split\n")
}

case class OraRowIdSplitStrategy(splitList : IndexedSeq[OraDBSplit],
                                 targetTable : TableAccessDetails
                                ) extends OraSplitStrategy {
  import OraSQLImplicits._

  private def sqlString(s : String) = literalsql(s"'${s}'")

  override def splitOraSQL(oraTblScan: OraTableScan, splitId: Int): Option[SQLSnippet] = {
    if (isTarget(oraTblScan, targetTable)) {
      val split = splitList(splitId).asInstanceOf[OraRowIdSplit]
      val cond = osql"rowid BETWEEN ${sqlString(split.start)} AND ${sqlString(split.stop)}"

      Some(
        SQLSnippet.subQuery(
          SQLSnippet.select(tableSelectList(oraTblScan): _*).
            from(tableFrom(oraTblScan)).
            where(cond)
        )
      )
    } else None
  }

  def explain(append: String => Unit): Unit = {
    append("Query split by row ids")

    append("\nTarget table:\n")
    targetTable.explain(append)

    append("\nSplits:\n")
    for (s <- splitList) {
      s.explain(append)
      append("\n")
    }

  }
}

case class OraPartitionSplitStrategy(splitList : IndexedSeq[OraDBSplit],
                                     targetTable : TableAccessDetails
                                    ) extends OraSplitStrategy {
  import OraSQLImplicits._

  override def splitOraSQL(oraTblScan: OraTableScan, splitId: Int): Option[SQLSnippet] = {
    if (isTarget(oraTblScan, targetTable)) {
      val split = splitList(splitId).asInstanceOf[OraPartitionSplit]
      val subPart : Boolean = targetTable.oraTable.isSubPartitioned

      def partClause(pNm : String) : SQLSnippet = {
        if (!subPart) {
          osql"PARTITION(${SQLSnippet.objRef(pNm)})"
        } else {
          osql"SUBPARTITION(${SQLSnippet.objRef(pNm)})"
        }
      }

      if (split.partitions.size == 1) {
        Some( osql"${tableFrom(oraTblScan)} ${partClause(split.partitions.head)}")
      } else {
        val selCl = SQLSnippet.select(tableSelectList(oraTblScan): _*)
        val fromCl = tableFrom(oraTblScan)
        val partSels = split.partitions.map{pNm =>
          SQLSnippet.select(tableSelectList(oraTblScan): _*).
            from(osql"${fromCl} ${partClause(pNm)}")
        }
        Some(
          SQLSnippet.subQuery(
            SQLSnippet.operator("union all", partSels : _*)
          )
        )
      }

    } else None
  }

  def explain(append: String => Unit): Unit = {
    append("Query split by partitions\n")

    append("\nTarget table:\n")
    targetTable.explain(append)

    append("\nSplits:\n")
    for (s <- splitList) {
      s.explain(append)
      append("\n")
    }


  }
}

case class OraResultSplitStrategy(splitList : IndexedSeq[OraDBSplit],
                                  scn : Long) extends OraSplitStrategy {
  override def splitOraSQL(oraTblScan: OraTableScan, splitId: Int): Option[SQLSnippet] = None

  override def associateFetchClause(sqlSnip: SQLSnippet,
                                    addOrderBy : Boolean,
                                    orderByCnt : Int,
                                    splitId : Int) : SQLSnippet = {
    import OraSQLImplicits._
    val split = splitList(splitId).asInstanceOf[OraResultSplit]

    val ordByCl = if (addOrderBy) {
      val ordrExprs = for (i <- 1 until  (orderByCnt + 1)) yield {
        SQLSnippet.literalSnippet(i.toString)
      }
      osql"${SQLSnippet.nl}order by ${SQLSnippet.csv(ordrExprs : _*)}"
    } else SQLSnippet.empty

    val offsetCl = {
      val offset = osql"OFFSET ${literalsql(split.offset)} ROWS"
      if (split.numRows != -1) {
        offset + osql" FETCH NEXT ${literalsql(split.numRows)} ROWS ONLY"
      } else offset
    }

    osql"${sqlSnip}${ordByCl}${SQLSnippet.nl}${offsetCl}"
  }

  def explain(append: String => Unit): Unit = {
    append("Query split by result rows")
    append(s"\nscn: ${scn}\n")

    append("\nSplits:\n")
    for (s <- splitList) {
      s.explain(append)
      append("\n")
    }

  }
}

/**
 * Algorithm for Splitting a Pushdown Query.
 *
 * '''Step 0:'''
 *  - If [[OraSparkConfig#ENABLE_ORA_QUERY_SPLITTING]] is false return
 *    [[NoSplitStrategy]]
 *
 * '''Step 1''' : analyze the [[OraPlan]] for splitting
 *  - run it through the [[QuerySplitAnalyzer]]
 *  - obtain the [[SplitScope]]
 *
 * '''Step 2:''' get Plan statistics from [[OraExplainPlan]]
 *  - get overall query `rowCount, bytes` and [[TableAccessOperation]]s if there
 *    is scope for [[OraPartitionSplit]], [[OraRowIdSplit]].
 *
 * '''Step 3:''' decide on SplitStrategy
 *  - If `planComplex && !resultSplitEnabled` then decide on [[NoSplitStrategy]]
 *    - A plan is complex if it has no candidate Tables for Splitting.
 *    - Since [[OraResultSplit]] may lead to lotof extra work on the Oracle instancw
 *      users can turn it off with the setting `spark.sql.oracle.allow.splitresultset`
 *  - Otherwise try to identify a table to split on based on [[SplitCandidate]]s.
 *    - Choose the table with the largest amount of bytes read.
 *    - Chosen table must have 10 times more bytes read than any other
 *      [[TableAccessOperation]]
 *  - Have the [[OraDBSplitGenerator]] generate splits based on query stats and optionally
 *    a target split table.
 *
 */
object OraSplitStrategy extends Logging with TimeIt {

  private def apply(splitList : IndexedSeq[OraDBSplit],
                    targettable : Option[TableAccessDetails],
                    currentSCN : Long
                   ) : OraSplitStrategy = {
     (splitList.headOption, targettable) match {
      case  (None, _) => NoSplitStrategy
      case (Some(s : OraNoSplit.type), _) => NoSplitStrategy
      case (Some(s : OraRowIdSplit), Some(tt)) => OraRowIdSplitStrategy(splitList, tt)
      case (Some(s : OraPartitionSplit), Some(tt)) => OraPartitionSplitStrategy(splitList, tt)
      case (Some(s : OraResultSplit), _) => OraResultSplitStrategy(splitList, currentSCN)
      case _ => NoSplitStrategy
    }
  }

  def generateSplits(dsKey : DataSourceKey,
                     oraPlan : OraPlan)(
      implicit sparkSession : SparkSession
  ) : (OraSplitStrategy, Option[PlanInfo]) = {
    val qrySplitEnabled = OraSparkConfig.getConf(OraSparkConfig.ENABLE_ORA_QUERY_SPLITTING)

    if (!qrySplitEnabled ) {
      (NoSplitStrategy, None)
    } else {
      val splitScope = QuerySplitAnalyzer.splitCandidates(oraPlan)
      val planComplex = splitScope.candidateTables.isEmpty
      val planInfoO = timeIt(
        s"""build Plan for: ${oraPlan.orasql.sql.substring(0, 30)}...
           |""".stripMargin)(
        OraExplainPlan.constructPlanInfo(dsKey, oraPlan, !planComplex)
      )
      val maxFetchTasks : Int = {
        val maxFetchRnds = OraSparkConfig.getConf(OraSparkConfig.MAX_SPLIT_FETCH_TASKS)
        Math.floor(OraSparkUtils.defaultParallelism(sparkSession) * maxFetchRnds).toInt
      }

      if (!planInfoO.isDefined) {
        (NoSplitStrategy, planInfoO)
      } else {
        val planInfo = planInfoO.get
        val bytesPerTask: Long = OraSparkConfig.getConf(OraSparkConfig.BYTES_PER_SPLIT_TASK)
        val resultSplitEnabled = OraSparkConfig.getConf(OraSparkConfig.ALLOW_SPLITBY_RESULTSET)

        if (planInfo.bytes < bytesPerTask || (planComplex && !resultSplitEnabled)) {
          (NoSplitStrategy, planInfoO)
        } else {
          val targetTable: Option[TableAccessDetails] =
            buildTableAccess(planInfo.tabAccesses, splitScope.candidateTables)
          val splitList: IndexedSeq[OraDBSplit] = new OraDBSplitGenerator(dsKey,
            planInfo.bytes,
            planInfo.rowCount,
            bytesPerTask,
            targetTable,
            maxFetchTasks
          ).generateSplitList
          val scn = ORASQLUtils.currentSCN(dsKey)
          (OraSplitStrategy(splitList, targetTable, scn), planInfoO)
        }
      }
    }
  }

  private def matchOraTable(tblAccess : TableAccessOperation,
                   splitTables : Seq[SplitCandidate]) : Option[OraTableScan] = {
    val chosenTable = splitTables.find {sp =>
      sp.oraTabScan.oraTable.name == tblAccess.tabNm &&
        (!sp.alias.isDefined || s"${sp.alias.get}@${tblAccess.qBlk}" == tblAccess.alias)
    }

    if (!chosenTable.isDefined) {
      logWarning(
        s"""Failed to match OraTable for tableAccess: ${tblAccess}
           |  spliTables = ${splitTables.mkString(",")}""".stripMargin
      )
    }

    chosenTable.map(_.oraTabScan)
  }

  private val LARGE_TABLE_BYTES_READ_MULTIPLIER = 10

  private def buildTableAccess(tabAccesses : Seq[TableAccessOperation],
                       splitTables : Seq[SplitCandidate]) : Option[TableAccessDetails] = {

    if (tabAccesses.nonEmpty) {
      val tbAcOp = tabAccesses.sortBy(tA => tA.bytes).last
      val isLargeTabAcc = tabAccesses.forall(tA => tA == tbAcOp ||
        tA.bytes < LARGE_TABLE_BYTES_READ_MULTIPLIER * tbAcOp.bytes
      )
      for (tblScan <- matchOraTable(tbAcOp, splitTables) if isLargeTabAcc) yield {
        tblScan.setQuerySplitCandidate
        TableAccessDetails(tblScan.oraTable, tbAcOp)
      }
    } else None
  }
}
