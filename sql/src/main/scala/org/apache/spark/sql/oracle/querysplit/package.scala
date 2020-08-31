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

import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.oracle.operators.OraTableScan

/**
 * '''Query Splitting''' is used to split a oracle pushdown query into a set of
 * queries such that the union-all of the results is the same as the original query result.
 *
 * Each split query is invoked in a seperate partition of the [[OraScan]].
 * Query splitting is trigerred when Spark requests the [[OraScan]] instance for
 * [[OraScan.planInputPartitions planInputPartitions]] or
 * [[OraScan.outputPartitioning outputPartitioning]].
 *
 * See further information on [[OraDBSplit schemes for splitting]],
 * [[QuerySplitAnalyzer what schemes apply to different classes of Queries]],
 * [[OraExplainPlan plan stats gathered]], and how [[OraSplitStrategy split analysis is done]].
 */
package object querysplit {

  case class TableAccessOperation(tabNm : String,
                                  qBlk : String,
                                  object_alias : String,
                                  alias : String,
                                  row_count : Long,
                                  bytes : Long,
                                  partitionRange : Option[(Int, Int)]) {

    def explain(append : String => Unit) : Unit = {
      if (!partitionRange.isDefined) {
        append(s"""name=${tabNm}, row_count=${row_count}, bytes=${bytes}""")
      } else {
        val (sP, eP) = partitionRange.get
        // scalastyle:off line.size.limit
        append(s"""name=${tabNm}, row_count=${row_count}, bytes=${bytes}, partitions=($sP, $eP)""")
        // scalastyle:on
      }
    }

  }

  case class ShardingPlanInfo(shardWorkersCost : Long,
                              shardWorkersTime : Long,
                              numShardWorkersQueries : Int,
                              totalCost : Long,
                              totalTime : Long,
                              hasJoins : Boolean,
                              hasTableAccess : Boolean) {
    def explain(append : String => Unit) : Unit = {
      // scalastyle:off line.size.limit
      append(s"Sharding details:\n")
      append(s"  shard instances cost=${shardWorkersCost}, total query cost=${totalCost}\n")
      append(s"  shard instances time(secs)=${shardWorkersTime}, total query time(secs)=${totalTime}\n")
      append(s"  num of shard queries in plan=${numShardWorkersQueries}, joins in coordinator=${hasJoins}, table scans in coordinator=${hasTableAccess}\n")
      // scalastyle:on
    }
  }

  case class PlanInfo(
                       rowCount : Long,
                       bytes : Long,
                       tabAccesses : Seq[TableAccessOperation],
                       shardingPlanInfo : Option[ShardingPlanInfo]
                     ) {
    def explain(append : String => Unit) : Unit = {
      append(s"rowCount = ${rowCount}, bytes=${bytes}\n")
      if (tabAccesses.nonEmpty) {
        append(s"split target candidates:\n")
        for (t <- tabAccesses) {
          t.explain(append)
          append("\n")
        }
      }
      if (shardingPlanInfo.isDefined) {
        shardingPlanInfo.get.explain(append)
      }
    }
  }

  def ORA_SYS_PARTITION(pNm : String) : Boolean = pNm.startsWith("SYS")

  case class TableAccessDetails(oraTable : OraTable, tabAccessOp : TableAccessOperation) {
    val scheme = oraTable.schema
    val name = oraTable.name

    /*
     * Try to form partition list from tableAccessOp; otherwise for partitioned tables
     * return all parts.
     */
    val tableParts : Option[Seq[String]] = {

      if (oraTable.isPartitioned) {

        val partNms = tabAccessOp.partitionRange.map { r =>
          val p = oraTable.partitions(r._1, r._2).filterNot(ORA_SYS_PARTITION)
          if (p.isEmpty) {
            oraTable.allPartitions.filterNot(ORA_SYS_PARTITION)
          } else {
            p
          }
        }.getOrElse(oraTable.allPartitions.filterNot(ORA_SYS_PARTITION))

        Some(partNms)
      } else {
        None
      }
    }

    def explain(append : String => Unit) : Unit = tabAccessOp.explain(append)
  }

  case class SplitCandidate(alias : Option[String],
                            oraTabScan : OraTableScan)

  case class SplitScope(candidateTables : Seq[SplitCandidate])


}
