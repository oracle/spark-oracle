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

package org.apache.spark.sql.oracle.sqlexec

import java.sql.{PreparedStatement, ResultSet}

import oracle.spark.DataSourceInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.oracle.{LoggingAndTimingSQL, OracleCatalogOptions}
import org.apache.spark.util.DoubleAccumulator

/**
 * Represent oracle jdbc statements issued from Spark Executors.
 * Provides Logging and Error tracking.
 * Based on StatementExecutor from scalikejdc and
 * [[org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils]]
 */
trait SparkOraStatement extends Logging { self =>
  import SparkOraStatement._

  def datasourceInfo : DataSourceInfo
  def underlying: PreparedStatement
  def sqlTemplate: String
  protected def isQuery : Boolean
  def bindValues: Seq[Literal]
  def catalogOptions: OracleCatalogOptions
  def timeToExecute: DoubleAccumulator

  private[this] val statementExecute = new Do with LogSQLAndTiming with LogSQLFailure

  private def performQuery : ResultSet = {
    val sTime = System.currentTimeMillis()
    val rs = underlying.executeQuery()
    val eTime = System.currentTimeMillis()
    timeToExecute.add(eTime - sTime)
    rs
  }

  def executeQuery(): java.sql.ResultSet =
    statementExecute(() => {
      performQuery
    })

  def executeUpdate(): Int = statementExecute(() => underlying.executeUpdate())

  protected def executeBatch : Unit = {
    val sTime = System.currentTimeMillis()
    statementExecute(() => underlying.executeBatch())
    val eTime = System.currentTimeMillis()
    timeToExecute.add(eTime - sTime)
  }

  private[this] lazy val sqlString: String = {
    try {
      if (isQuery) {
        SQLTemplate.buildPrintableSQL(sqlTemplate, bindValues)
      } else {
        sqlTemplate
      }
    } catch {
      case e: Exception =>
        log.debug("Caught an exception when formatting SQL because of " + e.getMessage)
        sqlTemplate
    }
  }

  private[this] def stackTraceInformation: String = {
    val logBehavior = catalogOptions.logSQLBehavior

    val stackTrace = Thread.currentThread.getStackTrace
    val lines = ({
      stackTrace.dropWhile { trace =>
        val className = trace.getClassName
        className != getClass.toString &&
        (className.startsWith("java.lang.") || className.startsWith("scalikejdbc."))
      }
    }).take(logBehavior.stackTraceDepth).map { trace =>
      "    " + trace.toString
    }

    s"""[Stack Trace]
      |...
      |$lines.mkString("\n")
      |...
      |""".stripMargin
  }

  private trait LogSQLFailure extends Execute {
    abstract override def apply[A](fn: () => A): A =
      try {
        super.apply(fn)
      } catch {
        case e: Exception =>
          logError(s"""SQL execution failed (Reason: ${e.getMessage} )
             |SQL is:
             |${sqlString}
             |Stack Trace:
             |${stackTraceInformation}
             |""".stripMargin)
          throw e
      }
  }

  private trait LogSQLAndTiming extends Execute {
    abstract override def apply[A](fn: () => A): A = {
      val before = System.currentTimeMillis
      val result = super.apply(fn)
      val after = System.currentTimeMillis
      val spentMillis = after - before

      val logIt = catalogOptions.logSQLBehavior.enabled ||
        (catalogOptions.logSQLBehavior.enableSlowSqlWarn &&
          catalogOptions.logSQLBehavior.slowSqlThreshold < spentMillis)

      if (logIt) {
        val logLevel = if (catalogOptions.logSQLBehavior.enabled) {
          catalogOptions.logSQLBehavior.logLevel
        } else {
          catalogOptions.logSQLBehavior.slowSqlLogLevel
        }

        def msg: String = {
          s"""SQL execution completed in ${spentMillis} millis
             |SQL is:
             |${sqlString}
             |""".stripMargin
        }
        import LoggingAndTimingSQL.LogLevel
        LoggingAndTimingSQL.LogLevel(logLevel) match {
          case LogLevel.INFO => self.logInfo(msg)
          case LogLevel.DEBUG => self.logDebug(msg)
          case LogLevel.WARNING => self.logWarning(msg)
          case LogLevel.ERROR => self.logError(msg)
          case LogLevel.TRACE => self.logTrace(msg)
        }

      }

      result
    }
  }

}

object SparkOraStatement {

  private trait Execute {
    def apply[A](fn: () => A): A
  }

  private class Do extends Execute {
    override def apply[A](fn: () => A): A = fn()
  }
}
