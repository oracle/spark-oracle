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

import java.sql.Connection
import java.util.concurrent.{ConcurrentHashMap => CMap}

import oracle.ucp.jdbc.PoolDataSource

import org.apache.spark.internal.Logging

/**
 * '''Connection Managment''' for Driver and Executor jvms.
 * Consumers should only use the ''getConnection(dsInfo : DataSourceInfo)'' method.
 *
 * Internally maintains a [[PoolDataSource]] per [[DataSourceKey]]; also maintains a
 * [[Connection]] with the current thread. On a ''getConnection'' call attempts to serve
 * the request from the COnnection associated with the calling thread; if not it returns
 * currrent connection(if open) back to its pool before associating a Connection of the
 * requested DataSourceKey with the calling Thread and returning it.
 *
 * Pool is setup using ''getNewPDS'' function from [[OracleDBConnectionUtils]] class
 * provided in ''oracle.hcat.db.conn'' code drop.
 * Pool set with `maxPoolSize = Runtime.availableProcessors()` and
 * `connectionWaitTime = 1 sec`.
 */
object ConnectionManagement extends DataSources with Logging {

  private val pdsMap = new CMap[DataSourceKey, PoolDataSource]()

  type THREAD_CONNECTION_TYPE = Option[(DataSourceKey, Connection)]

  private val _threadLocalConnection = new ThreadLocal[THREAD_CONNECTION_TYPE] {
    override def initialValue() = None
  }

  private def attachAndGetConnection(dsKey: DataSourceKey, pds: PoolDataSource): Connection = {
    _threadLocalConnection.get() match {
      case Some((tDsKey, conn)) if (tDsKey == dsKey && !conn.isClosed) => conn
      case None =>
        logDebug(s"Connection request for ${dsKey} on ${Thread.currentThread().getName}")
        val conn: Connection = pds.getConnection()
        _threadLocalConnection.set(Some(dsKey, conn))
        conn
      case Some((tDsKey, conn)) =>
        logDebug(
          s"Connection request for ${dsKey} on ${Thread.currentThread().getName}," +
            s" when thread has open connection for ${tDsKey}")
        if (!conn.isClosed) {
          conn.close()
        }
        _threadLocalConnection.set(None)
        attachAndGetConnection(dsKey, pds)
    }
  }

  /**
   * Not used, but if the strategy of closing the connection
   * as part of Task completion/failure( see [[OraPartitionReader.ConnectionCloser]])
   * has issue, than an alternate is to get a new connection for each oracle statement
   * executed on a Executor slot/thread.
   */
  private def getConnectionFromPool(dsKey: DataSourceKey, pds: PoolDataSource): Connection = {
    logDebug(s"Connection request for ${dsKey} on ${Thread.currentThread().getName}")
    val conn: Connection = pds.getConnection()
    conn
  }

  private def _setupPool(dsKey: DataSourceKey, connInfo: ConnectionInfo): PoolDataSource = {

    val createPDS = new java.util.function.Function[DataSourceKey, PoolDataSource] {
      override def apply(t: DataSourceKey): PoolDataSource = {
        val pds = OracleDBConnectionUtils.getNewPDS(
          connInfo.authMethod.getOrElse(null),
          connInfo.asConnectionProperties,
          connInfo.url)
        logInfo(s"Setting up Connection Pool ${dsKey}")

        /*
         * UCP follow-up
         * 1.
         * https://github.com/oracle/oracle-db-examples/tree/master/java/jdbc
         * BasicSamples
         * - no managing UCP pools: like setAbandonedConnectionTimeout
         * - also see https://docs.oracle.com/en/database/oracle/oracle-database/21/jjucp/UCP-best-practices.html#GUID-619C7C59-10F8-4035-A21B-DEAA826CD323
         *
         * 2. UCP logging
         * followup UCP guide logging chapter
         *
         * 3. Proxy Auth
         *  https://github.com/oracle/oracle-db-examples/tree/master/java/jdbc/ConnectionSamples
         *
         * 4.
         * https://blogs.oracle.com/dev2dev/some-recommendations-for-connection-leaks-investigations-and-pool-adjustments-in-the-ucp-based-applications
         */

        pds.setMaxPoolSize(Runtime.getRuntime.availableProcessors() + 2)
        pds.setConnectionWaitTimeout(1)
        pds.setValidateConnectionOnBorrow(true)
        // pds.setAbandonedConnectionTimeout(10)
        pds.setTimeoutCheckInterval(5)


        pds.setShardingMode(false)

        pds
      }
    }
    pdsMap.computeIfAbsent(dsKey, createPDS)
  }

  /**
   * For calls on the driver.
   * Now it is possible to have multiple interleaved Oracle calls.
   * For example: Loading a Function definition that has an Arg of User_Defined_Type.
   * This might cause a call [[OraTypes#loadType]].
   * So each call gets a Connection from the Pool.
   */
  private[oracle] def getConnection(
      dsKey: DataSourceKey,
      connInfo: ConnectionInfo): Connection = {
    val pds = _setupPool(dsKey, connInfo)
    // attachAndGetConnection(dsKey, pds)
    getConnectionFromPool(dsKey, pds)
  }

  /*
   * For Executor Tasks.
   * If the strategy of closing the connection
   * as part of Task completion/failure( see [[OraPartitionReader.ConnectionCloser]])
   * has issue, than an alternate is to get a new connection for each oracle statement
   * executed on a Executor slot/thread.
   */
  def getConnectionInExecutor(dsInfo: DataSourceInfo): Connection = {
    val pds = _setupPool(dsInfo.key, dsInfo.connInfo)
    attachAndGetConnection(dsInfo.key, pds)
  }

  def reset(): Unit = synchronized {
    import scala.collection.JavaConverters._
    for (pds <- pdsMap.values().asScala) {
      // TODO
    }
    pdsMap.clear()
  }

  /**
   * Only use for tests
   * @return
   */
  def getDSKeyInTestEnv : DataSourceKey = {
    assert(pdsMap.size() == 1)
    pdsMap.keys().nextElement()
  }

  /**
   * Only use for tests
   * @return
   */
  def getDSPoolInTestEnv : PoolDataSource = {
    assert(pdsMap.size() == 1)
    pdsMap.values().iterator().next()
  }

}
