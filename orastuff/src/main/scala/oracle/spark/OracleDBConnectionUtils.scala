/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.spark

import java.sql.SQLException
import java.util.{Locale, Properties}

import oracle.ucp.jdbc.{PoolDataSource, PoolDataSourceFactory}


object OracleDBConnectionUtils {

  private val SIMPLE_AUTH_METHOD = "SIMPLE"
  private val WALLET_AUTH_METHOD = "ORACLE_WALLET"


  @throws[SQLException]
  def getNewPDS(authMethodString: String, props: Properties, url: String): PoolDataSource = {

    val authMethod: String = Option(authMethodString).getOrElse(SIMPLE_AUTH_METHOD)

    authMethod.toUpperCase(Locale.ROOT) match {
      case SIMPLE_AUTH_METHOD => getRegularPDS(props, url)
      case WALLET_AUTH_METHOD => getWalletPDS(props, url)
      case _ => throw new SQLException("Invalid Authentication Method")
    }
  }

  @throws[SQLException]
  private def getRegularPDS(props: Properties, url: String): PoolDataSource = {
    val poolDS: PoolDataSource = PoolDataSourceFactory.getPoolDataSource
    poolDS.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource")
    poolDS.setURL(url)
    poolDS.setMaxPoolSize(1)
    poolDS.setConnectionProperties(props)

    poolDS
  }

  private val ORACLE_NET_TNS_ADMIN: String = "oracle.net.tns_admin"

  @throws[SQLException]
  private def getWalletPDS(props: Properties, url: String): PoolDataSource = {

    if (System.getProperty(OracleDBConnectionUtils.ORACLE_NET_TNS_ADMIN) == null) {
      if (props.get(OracleDBConnectionUtils.ORACLE_NET_TNS_ADMIN) != null) {
        System.setProperty(
          OracleDBConnectionUtils.ORACLE_NET_TNS_ADMIN,
          props.get(OracleDBConnectionUtils.ORACLE_NET_TNS_ADMIN).asInstanceOf[String])
      } else {
        throw new SQLException("TNS_ADMIN not set")
      }
    }
    val walletPDS: PoolDataSource = PoolDataSourceFactory.getPoolDataSource

    walletPDS.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource")
    walletPDS.setURL(url)
    walletPDS.setInitialPoolSize(1)
    walletPDS.setMaxPoolSize(1)
    walletPDS.setMinPoolSize(1)
    walletPDS.setConnectionProperties(props)
    walletPDS.setPassword(null)

    walletPDS
  }
}
