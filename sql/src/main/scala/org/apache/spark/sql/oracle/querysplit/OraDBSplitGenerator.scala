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

import scala.collection.mutable.ArrayBuffer

import oracle.spark.{DataSourceKey, ORASQLUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.oracle.util.TimeIt

/**
 * Following are the ways in which we split a pushdown query:
 *  - [[OraResultSplit]]
 *    split based on the resultset using the fetch clause:
 *    ` OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`
 *  - [[OraPartitionSplit]]
 *    split by partition/subpartition using the partition_extension_clause
 *    `table_ref partition(pName)` or `tableref subpartition(subPName)`
 *  - [[OraRowIdSplit]]
 *    split by `rowId` ranges using the predicate
 *    ` rowid BETWEEN ? AND ?`
 */
trait OraDBSplit {
  def explain(append : String => Unit) : Unit
}

case object OraNoSplit extends OraDBSplit {
  def explain(append : String => Unit) : Unit = ()
}

case class OraResultSplit(offset : Long, numRows : Long) extends OraDBSplit {
  def explain(append : String => Unit) : Unit = {
    append(s"offset = ${offset}, numRows=${numRows}")
  }
}

case class OraPartitionSplit(partitions : Seq[String]) extends OraDBSplit {
  def explain(append : String => Unit) : Unit = {
    append(s"partitions = ${partitions.mkString(",")}")
  }
}

case class OraRowIdSplit(start : String, stop : String) extends OraDBSplit {
  def explain(append : String => Unit) : Unit = {
    append(s"start-rowId = ${start}, end-rowId=${stop}")
  }
}

/**
 * Given a Query's output stats(bytes, rowCOunt), the maximum number of fetch tasks,
 * and a potential targetTable the splits are created using the following rules and procedures:
 *
 * {{{
 *   degree_of_parallelism = outputBytes / bytesPerTask
 *   numSplits = Math.min(
 *     Math.ceil(degree_of_parallel).toInt,
 *     Math.max(maxFetchTasks, 2)
 *   )
 *   rowsPerSplit = Math.ceil(rowCount / numSplits.toDouble).toInt
 * }}}
 *
 *  - If `dop <= 1` return a split List of 1 containing the [[OraNoSplit]] split.
 *  - If there is no `target table` split by result_set into a list of [[OraResultSplit]]
 *    Each split contains `rowsPerSplit`. For example if the estimate `10150` rows
 *    and we estimate `12` splits then each split contains `846` rows(the last one has 2 less).
 *    If the stat estimates are high then some of the last Splits may contain no rows, on the
 *    other hand a low estimate may lead to the last Split fetching a lot of data.
 *  - When there is a `target table` and the [[TableAccessDetails]] has a list of
 *    targeted partitions, we distribute the partitions into `numSplits`. This
 *    assumes partitions are approximately the same size. Generate a list of
 *    [[OraPartitionSplit]] where each Split contains one or more partitions.
 *    List of targeted partions is preferably inferred in the plan. But
 *    if target table is a partitioned table and the plan doesn't infer target partitions,
 *    we set target partitions to all partitions of the target table. This is
 *    because, splitting a partitioned table by partitions is preferred to splitting it
 *    by rowid.
 *  - When there is a `target table` and there is no list of targeted partitions
 *    we split based on `rowId` ranges. We use the [[https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_PARALLEL_EXECUTE.html#GUID-D13B6975-09B5-4711-AD43-45F68228C1CC DBMS_PARALLEL_PACKAGE]]
 *    to compute row chunks. The user of the Catalog connection must have
 *    `CREATE JOB system privilege`. The Splits are setup by invoking
 *    `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID` with `chunk_size = rowsPerSplit`
 *
 * @param dsKey
 * @param outputBytes
 * @param rowCount
 * @param bytesPerTask
 * @param targetTableO
 */
class OraDBSplitGenerator(dsKey : DataSourceKey,
                           outputBytes : Long,
                           rowCount : Long,
                           bytesPerTask : Long,
                           targetTableO : Option[TableAccessDetails],
                          maxFetchTasks : Int)
  extends Logging with TimeIt {

  import OraDBSplitGenerator._

  private lazy val degree_of_parallel : Double = outputBytes / bytesPerTask.toDouble

  private lazy val numSplits : Int = Math.min(
    Math.ceil(degree_of_parallel).toInt,
    Math.max(maxFetchTasks, 2)
  )

  private def noSplitList = IndexedSeq(OraNoSplit)

  private def resultSplitList : IndexedSeq[OraDBSplit] = {
    val rowsPerSplit = Math.ceil(rowCount / numSplits.toDouble).toInt

    val splits = new Array[OraResultSplit](numSplits)
    var rownum = 0
    var i = 0
    while (i < numSplits) {
      splits(i) = OraResultSplit(rownum, rowsPerSplit)
      i += 1
      rownum += rowsPerSplit
    }

    /*
     * The last Split fetches the remaining rows
     */
    splits(numSplits - 1) = splits(numSplits -1).copy(numRows = -1)

    splits.toIndexedSeq
  }

  private def partitionSplitList(table : TableAccessDetails,
                                 partitions : Seq[String]) : IndexedSeq[OraDBSplit] = {
    val numPartitions = partitions.length
    val actualSplits = Math.min(numPartitions, numSplits)
    val partsPerSplit = Math.ceil(numPartitions / actualSplits.toDouble).toInt
    val splits = ArrayBuffer[OraPartitionSplit]()

    for((splitParts, i) <- partitions.sliding(partsPerSplit, partsPerSplit).zipWithIndex) {
      splits += OraPartitionSplit(splitParts)
    }

    splits.toIndexedSeq
  }

  /**
   * Manual sql example:
   * {{{
   *   SQL> select DBMS_PARALLEL_EXECUTE.GENERATE_TASK_NAME('ORASPARK') from dual;
   *
   * DBMS_PARALLEL_EXECUTE.GENERATE_TASK_NAME('ORASPARK')
   * --------------------------------------------------------------------------------
   * ORASPARK5
   *
   * SQL> exec DBMS_PARALLEL_EXECUTE.CREATE_TASK('ORASPARK5', 'Dummy task for ORASPARK splitter');
   *
   * PL/SQL procedure successfully completed.
   *
   * SQL> exec DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(
   *    'ORASPARK5', 'SPARKTEST', 'UNIT_TEST', TRUE, 500);
   *
   * PL/SQL procedure successfully completed.
   *
   * SQL> select START_ROWID, END_ROWID
   * from user_parallel_execute_chunks where task_name='ORASPARK5' order by START_ROWID;
   *
   * START_ROWID        END_ROWID
   * ------------------ ------------------
   * AAASH2AAMAAAKYwAAA AAASH2AAMAAAKZPH//
   *
   * SQL> exec DBMS_PARALLEL_EXECUTE.DROP_CHUNKS('ORASPARK5')
   *
   * PL/SQL procedure successfully completed.
   *
   * SQL> exec DBMS_PARALLEL_EXECUTE.DROP_TASK('ORASPARK5')
   *
   * PL/SQL procedure successfully completed.
   * }}}
   *
   * @param table
   * @return
   */
  private def rowIdSplitList(table : TableAccessDetails) : IndexedSeq[OraDBSplit] = {
    /**
     * Try to use the table count estimate. Why?
     * - the plan estimate of bytes and rows is based on returned rows.
     * - whereas rowid ranges are based on the entire table.
     * For example:
     * {{{
     * table rowCount = 1000, query result_cnt = 100, bytes = 1000, bytesPerTask=100
     * So:
     * dop=10
     * if we use query row_count, we get rowsPerSplit=10 => rowsPerSplit=10, so we get 100 tasks
     * Using table rowCount=1000 => rowsPerSplit=100, so we get 10 tasks.
     * }}}
     */
    val rowsPerSplit = {
      var rCnt = table.oraTable.tabStats.row_count.getOrElse(rowCount)
      // in case tableStats have stale/missing rowCount, use plan's rowCount.
      if (rowCount > rCnt) {
        rCnt = rowCount
      }
      Math.ceil(rCnt / numSplits.toDouble).toInt
    }

    timeIt(s"Construct row splits for table '${table.name}'")(
    ORASQLUtils.perform(
      dsKey,
      s"build row-id splits for table ${table.scheme}.${table.name}"
    ) {conn =>
      val taskName = ORASQLUtils.performQuery(conn, CHUNK_TASK_NAME_SQL) {rs =>
        rs.next()
        rs.getString(1)
      }

      ORASQLUtils.performQuery(conn, CHUNK_TASK_CREATE_TASK_SQL,
        ps => ps.setString(1, taskName))(_ => ())

      ORASQLUtils.performCall(conn,
        CHUNK_TASK_CREATE_CHUNKS,
        cs => {
          cs.setString(1, taskName)
          cs.setString(2, table.scheme)
          cs.setString(3, table.name)
          cs.setLong(4, rowsPerSplit)
        }
      )

      val splitList = ORASQLUtils.performQuery(conn,
        ROWID_CHUNKS_QUERY,
        ps => ps.setString(1, taskName)
      ) { rs =>
        val ab = ArrayBuffer[OraRowIdSplit]()
        while (rs.next) {
          ab += OraRowIdSplit(rs.getString(1), rs.getString(2))
        }
        ab.toIndexedSeq
      }

      ORASQLUtils.performQuery(conn, CHUNK_TASK_DROP_CHUNKS,
        ps => ps.setString(1, taskName))(_ => ())
      ORASQLUtils.performQuery(conn, CHUNK_TASK_DROP_TASK,
        ps => ps.setString(1, taskName))(_ => ())

      splitList
    }
    )
  }

  def generateSplitList : IndexedSeq[OraDBSplit] = {
    if (numSplits <= 1) {
      noSplitList
    } else if (!targetTableO.isDefined) {
      resultSplitList
    } else {
      val targetTable = targetTableO.get
      if (targetTable.tableParts.isDefined) {
        partitionSplitList(targetTable, targetTable.tableParts.get)
      } else {
        rowIdSplitList(targetTable)
      }
    }
  }

}

object OraDBSplitGenerator {

  private[querysplit] val CHUNK_TASK_NAME_SQL =
    """select "SYS"."DBMS_PARALLEL_EXECUTE"."GENERATE_TASK_NAME"('ORASPARK') from dual"""

  private[querysplit] val CHUNK_TASK_CREATE_TASK_SQL =
    """call "SYS"."DBMS_PARALLEL_EXECUTE"."CREATE_TASK"(?,'Dummy task for ORASPARK splitter')"""

  private[querysplit] val CHUNK_TASK_DROP_CHUNKS =
    """call "SYS"."DBMS_PARALLEL_EXECUTE"."DROP_CHUNKS"(?)"""

  private[querysplit] val CHUNK_TASK_DROP_TASK =
    """call "SYS"."DBMS_PARALLEL_EXECUTE"."DROP_TASK"(?)"""

  private[querysplit] val CHUNK_TASK_CREATE_CHUNKS =
  """begin
      |  "SYS"."DBMS_PARALLEL_EXECUTE"."CREATE_CHUNKS_BY_ROWID"(?,?,?,TRUE,?);
      |end;""".stripMargin

  private[querysplit] val ROWID_CHUNKS_QUERY =
  """
      |select "START_ROWID", "END_ROWID"
      |from "SYS"."USER_PARALLEL_EXECUTE_CHUNKS"
      |where "TASK_NAME"=?
      |order by "START_ROWID"""".stripMargin
}
