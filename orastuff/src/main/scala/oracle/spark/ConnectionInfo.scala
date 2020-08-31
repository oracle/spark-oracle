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

import java.util.{Locale, Properties}

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class ConnectionInfo(
    url: String,
    username: String,
    password: Option[String],
    sunPrincipal: Option[String],
    kerberosCallback: Option[String],
    krb5Conf: Option[String],
    tnsAdmin: Option[String],
    authMethod: Option[String]) {

  def this(url: String, username: String, password: String) =
    this(url, username, Some(password), None, None, None, None, None)

  private[oracle] def dump: String = {
    def toString(v: Any): String = v match {
      case Some(x) => x.toString
      case _ => v.toString
    }
    val values = productIterator
    val m = getClass.getDeclaredFields.map(_.getName -> toString(values.next)).toMap
    m.toSeq.map(t => s"${t._1} = ${t._2}").mkString("\t", "\n\t", "\n")
  }

  lazy val asConnectionProperties: Properties = {
    val properties = new Properties()
    import ConnectionInfo._

    def orclPropName(nm: String) = ORCL_KEY_MAP(nm)

    properties.setProperty(orclPropName(ORACLE_URL), url)
    properties.setProperty(orclPropName(ORACLE_JDBC_USER), username)

    for (p <- password) {
      properties.setProperty(orclPropName(ORACLE_JDBC_PASSWORD), p)
    }

    for (sp <- sunPrincipal) {
      properties.setProperty(orclPropName(SUN_SECURITY_KRB5_PRINCIPAL), sp)
    }
    for (kc <- kerberosCallback) {
      properties.setProperty(orclPropName(KERB_AUTH_CALLBACK), kc)
    }
    for (kc <- krb5Conf) {
      properties.setProperty(orclPropName(JAVA_SECURITY_KRB5_CONF), kc)
    }
    for (ta <- tnsAdmin) {
      properties.setProperty(orclPropName(ORACLE_NET_TNS_ADMIN), ta)
    }
    for (am <- authMethod) {
      properties.setProperty(orclPropName(ORACLE_JDBC_AUTH_METHOD), am)
    }

    properties
  }

  def convertToShard(shardURL : String) : ConnectionInfo = {
    this.copy(url = shardURL)
  }

}

object ConnectionInfo {
  private val connOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    connOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val ORACLE_URL = newOption("url")
  val ORACLE_JDBC_USER = newOption("user")
  val ORACLE_JDBC_PASSWORD = newOption("password")
  val SUN_SECURITY_KRB5_PRINCIPAL = newOption("sun.security.krb5.principal")
  val KERB_AUTH_CALLBACK = newOption("kerbCallback")
  val JAVA_SECURITY_KRB5_CONF = newOption("java.security.krb5.conf")
  val ORACLE_NET_TNS_ADMIN = newOption("net.tns_admin")
  val ORACLE_JDBC_AUTH_METHOD = newOption("authMethod")

  val ORCL_KEY_MAP = Map(
    ORACLE_URL -> ORACLE_URL,
    ORACLE_JDBC_USER -> ORACLE_JDBC_USER,
    ORACLE_JDBC_PASSWORD -> ORACLE_JDBC_PASSWORD,
    SUN_SECURITY_KRB5_PRINCIPAL -> SUN_SECURITY_KRB5_PRINCIPAL,
    KERB_AUTH_CALLBACK -> "oracle.hcat.osh.kerb.callback",
    JAVA_SECURITY_KRB5_CONF -> JAVA_SECURITY_KRB5_CONF,
    ORACLE_NET_TNS_ADMIN -> "oracle.net.tns_admin",
    ORACLE_JDBC_AUTH_METHOD -> "oracle.hcat.osh.authentication")

  val DEFAULT_MAX_SPLITS = 1

  def connectionInfo(parameters: CaseInsensitiveMap[String]): ConnectionInfo = {
    require(parameters.isDefinedAt(ORACLE_URL), s"Option '$ORACLE_URL' is required.")
    require(parameters.isDefinedAt(ORACLE_JDBC_USER), s"Option '$ORACLE_JDBC_USER' is required.")

    ConnectionInfo(
      parameters(ORACLE_URL),
      parameters(ORACLE_JDBC_USER),
      parameters.get(ORACLE_JDBC_PASSWORD),
      parameters.get(SUN_SECURITY_KRB5_PRINCIPAL),
      parameters.get(KERB_AUTH_CALLBACK),
      parameters.get(JAVA_SECURITY_KRB5_CONF),
      parameters.get(ORACLE_NET_TNS_ADMIN),
      parameters.get(ORACLE_JDBC_AUTH_METHOD))
  }
}
