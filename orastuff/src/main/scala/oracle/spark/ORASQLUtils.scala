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

package oracle.spark

import java.sql.{CallableStatement, Connection, PreparedStatement, ResultSet, Statement}

import oracle.jdbc.internal.OracleConnection
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.oracle.OraSparkUtils

object ORASQLUtils extends Logging {

  private def actionMessage(dsKey: DataSourceKey, actionDetails: String) =
    s"Failed ${actionDetails} on ${dsKey}"

  def withConnection[V](dsKey: DataSourceKey, conn: Connection, actionDetails: => String)(
      action: Connection => V): V = {
    try {
      logDebug(actionDetails)
      action(conn)
    } catch {
      case ex: Throwable =>
        val errMsg = actionMessage(dsKey, actionDetails)
        logError(errMsg, ex)
        throw new RuntimeException(errMsg, ex)
    } finally {
      conn.close
    }
  }

  /*
   * public for testing purposes only
   */
  def getConnection(dsKey: DataSourceKey): Connection = {
    try {
      ConnectionManagement.getConnection(dsKey)
    } catch {
      case ex: Throwable =>
        val errMsg = actionMessage(dsKey, "get connection")
        logError(errMsg, ex)
        throw new RuntimeException(errMsg, ex)
    }
  }

  /**
   * perform an '''action''' on the given DataSource. A [[Connection]] is obtained
   * for the DataSource and the action is invoked with the connection.
   * Before returning the Connection is closed.
   *
   * @param dsKey key of the DataSource
   * @param actionDetails If action fails a RuntimeException is thrown with this string
   * @param action a callback that is called with a DataSource Connection.
   * @tparam V return type of the action callback
   * @return value returned by the action callback.
   */
  def perform[V](dsKey: DataSourceKey, actionDetails: => String)(action: Connection => V): V = {
    withConnection(dsKey, getConnection(dsKey), s"${actionDetails} on ${dsKey}")(action)
  }

  private def performOrLogFailure[T](action: => T, logMsg: => String): T = {
    Try {
      action
    }.recover[T] {
        case tx: Throwable =>
          logError(logMsg, tx)
          null.asInstanceOf[T]
      }
      .get
  }

  def performQuery[V](
      conn: Connection,
      stmt: => String,
      setStmtParams: PreparedStatement => Unit = ps => ())(action: ResultSet => V): V = {
    var pStmt: PreparedStatement = null
    var rs: ResultSet = null

    try {
      pStmt = conn.prepareStatement(stmt)
      setStmtParams(pStmt)
      rs = pStmt.executeQuery()
      action(rs)
    } finally {
      performOrLogFailure(
        {
          if (rs != null) rs.close()
          ()
        },
        s"""Failed to close resultSet for statment:
                ${stmt}
          """.stripMargin)

      performOrLogFailure(
        {
          if (pStmt != null) pStmt.close()
          ()
        },
        s"""Failed to close statment:
                ${stmt}
          """.stripMargin)
    }

  }

  def performDML[V](
      conn: Connection,
      stmt: => String,
      setStmtParams: PreparedStatement => Unit = ps => ()): Array[Int] = {
    var pStmt: PreparedStatement = null

    try {
      pStmt = conn.prepareStatement(stmt)
      setStmtParams(pStmt)
      pStmt.addBatch()
      pStmt.executeBatch()
    } finally {
      performOrLogFailure(
        {
          if (pStmt != null) pStmt.close()
          ()
        },
        s"""Failed to close statment:
                ${stmt}
          """.stripMargin)
    }
  }

  def performBatchDML[V](
                     conn: Connection,
                     stmt: => String,
                     initPStmt : PreparedStatement => Unit,
                     batchItr: Iterator[PreparedStatement => Unit],
                     batchSize : Int): Array[Int] = {
    var pStmt: PreparedStatement = null

    try {
      pStmt = conn.prepareStatement(stmt)
      initPStmt(pStmt)
      var rowCount = 0
      val retCounts = ArrayBuffer[Int]()
      while (batchItr.hasNext) {
        batchItr.next()(pStmt)
        pStmt.addBatch()
        rowCount += 1
        if (rowCount % batchSize == 0) {
          retCounts ++= pStmt.executeBatch()
          rowCount = 0
        }
      }
      if (rowCount > 0) {
        retCounts ++= pStmt.executeBatch()
      }
      retCounts.toArray
    } finally {
      performOrLogFailure(
        {
          if (pStmt != null) pStmt.close()
          ()
        },
        s"""Failed to close statment:
                ${stmt}
          """.stripMargin)
    }
  }

  def performSQL(conn: Connection, sql: => String): Boolean = {
    var stmt: Statement = null
    try {
      stmt = conn.createStatement()
      stmt.execute(sql)
    } finally {
      performOrLogFailure(
        {
          if (stmt != null) stmt.close()
          ()
        },
        s"""Failed to close statment :
                ${stmt}
          """.stripMargin)
    }
  }

  def performCall(
      conn: Connection,
      stmt: => String,
      setInParams: CallableStatement => Unit = cs => (),
      getOutParams: CallableStatement => Unit = cs => ()): Boolean = {
    var cStmt: CallableStatement = null

    try {
      cStmt = conn.prepareCall(stmt)
      setInParams(cStmt)
      val r = cStmt.execute()
      getOutParams(cStmt)
      r
    } finally {
      performOrLogFailure(
        {
          if (cStmt != null) cStmt.close()
          ()
        },
        s"""Failed to close statment:
                ${stmt}
          """.stripMargin)
    }
  }

  def performDDL[V](
                     conn: Connection,
                     stmt: => String
                   ): Int = {
    var pStmt: PreparedStatement = null

    try {
      pStmt = conn.prepareStatement(stmt)
      pStmt.executeUpdate()
    } finally {
      performOrLogFailure(
        {
          if (pStmt != null) pStmt.close()
          ()
        },
        s"""Failed to close statment:
                ${stmt}
          """.stripMargin)
    }
  }

  /**
   * Perform a Query on a DataSource, and invoke the callback function on the
   * [[ResultSet]]. Handle connection, statement and resultSet managment.
   *
   * @param dsKey the DataSource to invoke the query on.
   * @param stmt  the sql Query.
   * @param actionDetails  used to log errors and message in thrown Exception.
   * @param setStmtParams caallback to set any Params on the [[PreparedStatement]]
   * @param action callback that is passed the ResultSet
   * @tparam V result type of the callback
   * @return value returned from the callback
   */
  def performDSQuery[V](
      dsKey: DataSourceKey,
      stmt: => String,
      actionDetails: => String,
      setStmtParams: PreparedStatement => Unit = ps => ())(action: ResultSet => V): V = {
    perform[V](dsKey, s"Performing ${actionDetails} by running Query:\n ${stmt}") { conn =>
      performQuery(conn, stmt, setStmtParams)(action)
    }
  }

  def performDSDML(
      dsKey: DataSourceKey,
      stmt: => String,
      actionDetails: => String,
      setStmtParams: PreparedStatement => Unit = ps => ()): Array[Int] = {
    perform[Array[Int]](dsKey, s"Performing ${actionDetails} by executing DML:\n ${stmt}") {
      conn =>
        performDML(conn, stmt, setStmtParams)
    }
  }

  def performDSBatchDML(
                    dsKey: DataSourceKey,
                    stmt: => String,
                    actionDetails: => String,
                    initPStmt : PreparedStatement => Unit,
                    batchItr: Iterator[PreparedStatement => Unit],
                    batchSize : Int): Array[Int] = {
    perform[Array[Int]](dsKey, s"Performing ${actionDetails} by executing Batch DML:\n '${stmt}") {
      conn =>
        performBatchDML(conn, stmt, initPStmt, batchItr, batchSize)
    }
  }

  def performDSCall(
      dsKey: DataSourceKey,
      stmt: => String,
      actionDetails: => String,
      setInParams: CallableStatement => Unit = cs => (),
      getOutParams: CallableStatement => Unit = cs => ()): Boolean = {
    perform[Boolean](dsKey, s"Performing ${actionDetails} by executing DML:\n '${stmt}") { conn =>
      performCall(conn, stmt, setInParams, getOutParams)
    }
  }

  def performDSDDL(
                    dsKey: DataSourceKey,
                    stmt: => String,
                    actionDetails: => String): Int = {
    perform[Int](dsKey, s"Performing ${actionDetails} by executing DDL:\n '${stmt}") {
      conn => performDDL(conn, stmt)
    }
  }

  def performDSSQL(dsKey: DataSourceKey, stmt: => String, actionDetails: => String): Boolean = {
    perform[Boolean](dsKey, s"Performing ${actionDetails}SQL by running Statement:\n '${stmt}") {
      conn =>
        performSQL(conn, stmt)
    }
  }

  def performDSSQLsInTransaction(dsKey: DataSourceKey,
                                 stmts : Seq[String],
                                 actionDetails: => String
                                ): Unit = {
    perform[Unit](dsKey,
      s"Performing ${actionDetails}, by running Statements:\n${stmts.mkString("\n")}"
      ) { conn =>
      conn.setAutoCommit(false)
      try {
        for (sql <- stmts) {
          performSQL(conn, sql)
        }
        conn.commit()
      } finally {
        conn.setAutoCommit(true)
      }
    }
  }

  val throwAnalysisException = OraSparkUtils.throwAnalysisException _

  def currentSCN(dsKey : DataSourceKey) : Long = {
    perform(dsKey, "Get SCN") {conn =>
      val internalConnection : OracleConnection = conn.unwrap(classOf[OracleConnection])
      internalConnection.getCurrentSCN()
    }
  }

}
