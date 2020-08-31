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
package org.apache.spark.sql.connector.read.oracle

import java.sql.{Connection, PreparedStatement, ResultSet}

import oracle.spark.{ConnectionManagement, DataSourceInfo}
import scala.util.control.NonFatal

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, SpecificInternalRow}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.read.oracle.OraPartition.OraQueryAccumulators
import org.apache.spark.sql.oracle.OracleCatalogOptions
import org.apache.spark.sql.oracle.expressions.{JDBCGetSet, OraLiterals}
import org.apache.spark.sql.oracle.expressions.OraLiterals.jdbcGetSet
import org.apache.spark.sql.oracle.sqlexec.SparkOraStatement
import org.apache.spark.util.{DoubleAccumulator, NextIterator, TaskCompletionListener, TaskFailureListener}

case class OraPartitionReader(
    oraPart: OraPartition,
    catalystOutput: Seq[Attribute],
    accumulators: OraQueryAccumulators)
    extends PartitionReader[InternalRow]
    with Logging { self =>

  private[this] var closed = false

  private[this] val oraQuery = OraQueryStatement(oraPart, accumulators.timeToFirstRow)

  private[this] val rs: ResultSet = oraQuery.executeQuery()

  val itr = new NextIterator[InternalRow] {

    private[this] val getters: Seq[JDBCGetSet[_]] =
      catalystOutput.map { attr =>
        jdbcGetSet(attr.dataType)
      }
    private[this] val mutableRow = new SpecificInternalRow(catalystOutput.map(x => x.dataType))

    override protected def getNext(): InternalRow = {
      if (rs.next()) {
        accumulators.rowsRead.add(1L)
        var i = 0
        while (i < getters.length) {
          getters(i).readValue(rs, i, mutableRow)
          i = i + 1
        }
        mutableRow
      } else {
        finished = true
        null.asInstanceOf[InternalRow]
      }
    }

    override def close(): Unit = self.close

  }

  override def next(): Boolean = itr.hasNext

  override def get(): InternalRow = itr.next()

  override def close(): Unit = {
    if (closed) return
    try {
      if (null != rs) {
        rs.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing resultset", e)
    }

    val conn = oraQuery.getConn
    try {
      if (null != conn) {
        oraQuery.underlying.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    /*
     * Connection closing handled by ConnectionCloser
     */
    closed = true
  }

}

/**
 * Spark setups [[ZippedPartitionsRDD2]] whose partition [[ZippedPartitionsPartition]]
 * contains the partitions of its contained [[RDD]]s. This can be recursive:
 * a [[ZippedPartitionsRDD2]] may contain [[ZippedPartitionsRDD2]].
 * So we could end up with a [[ZippedPartitionsPartition]] that contains several
 * oracle [[org.apache.spark.sql.execution.datasources.v2.DataSourceRDD]].
 *
 * Each [[ZippedPartitionsRDD2]]'s [[ZippedPartitionsPartition]] is evaluated
 * as 1 Task, which is performed on 1 executor thread. So you end up with a
 * situation where an Executor Thread is simultaneously executing multiple
 * oracle sqls.
 *
 * In this case sharing the connection causes issues, because
 * we issue a `Statement, Connection` close when the `NextIterator` in the
 * [[OraPartitionReader]] is finished. Closing the Connection closes all statements
 * associated with the Connection, causing subsequent `ResultSet.next` calls in this
 * Task against the other statements) to fail.
 *
 * Where does the [[ZippedPartitionsRDD2]] get setup?
 * - see [[org.apache.spark.sql.execution.WholeStageCodegenExec.doExecute]] method.
 *   - line 764, if there are 2 child RDDs they are zipped.
 *   - in case of a [[org.apache.spark.sql.execution.joins.SortMergeJoinExec]] there
 *     are 2 input/child RDDs which share the same data distribution and numOfPartitions.
 *
 * So we close the connection when the Task finishes/fails and not when the
 * resultset is finished.
 *
 * @param conn
 */
case class ConnectionCloser(conn : Connection)
  extends TaskCompletionListener with TaskFailureListener with Logging {

  private def close : Unit = {
    try {
      if (null != conn) {
        if (!conn.isClosed && !conn.getAutoCommit) {
          try {
            conn.commit()
          } catch {
            case NonFatal(e) => logWarning("Exception committing transaction", e)
          }
        }
        conn.close()
      }
      logInfo("closed connection")
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
  }

  override def onTaskCompletion(context: TaskContext): Unit = close
  override def onTaskFailure(context: TaskContext, error: Throwable): Unit = close
}

case class OraQueryStatement(oraPart: OraPartition, timeToExecute: DoubleAccumulator)
    extends SparkOraStatement {

  import oraPart._

  private var conn : Connection = null

  lazy val sqlTemplate: String = oraPartSQL
  lazy val bindValues: Seq[Literal] = oraPartSQLParams
  lazy val datasourceInfo : DataSourceInfo = dsInfo
  protected val isQuery : Boolean = true

  lazy val underlying: PreparedStatement = {
    conn = {
      val c = ConnectionManagement.getConnectionInExecutor(dsInfo)
      val tc = TaskContext.get()
      if (tc != null) {
        val l = ConnectionCloser(c)
        tc.addTaskCompletionListener(l)
        tc.addTaskFailureListener(l)
      } else {
        throw new IllegalStateException(
          "setting up a OraQueryStatement.preparedStatement on a thread with no TaskContext"
        )
      }
      c
    }
    val ps =
      conn.prepareStatement(sqlTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    ps.setFetchSize(dsInfo.catalogOptions.fetchSize)
    bindValues(ps)
    ps
  }

  override def catalogOptions: OracleCatalogOptions = dsInfo.catalogOptions

  private def bindValues(ps: PreparedStatement): Unit = {
    OraLiterals.bindValues(ps, bindValues)
  }

  private[oracle] def getConn : Connection = conn
}
