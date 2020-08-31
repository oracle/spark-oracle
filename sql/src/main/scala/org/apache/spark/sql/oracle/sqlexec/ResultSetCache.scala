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

import java.io.{File, FileInputStream, FileOutputStream}
import java.sql.ResultSet

import scala.util.Try

import oracle.jdbc.rowset.OracleWebRowSet
import oracle.spark.DataSourceInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.oracle.OracleCatalogOptions
import org.apache.spark.util.Utils

/**
 * This is DISABLED. No more `OracleCatalogOptions.use_resultset_cache`
 *
 * If [[OracleCatalogOptions.use_resultset_cache]] is enabled
 * [[ResultSet]]s are saved and served from a local folder.
 *
 */
object ResultSetCache extends  Logging {

  private var cacheLoc : File = null

  def initCacheLoc(catalogOptions: OracleCatalogOptions) : File = synchronized {
    throw new UnsupportedOperationException("ResultSetCache no more supported")
  }

  /*
   * Wanted to setup this behavior.
   * But adding dependency: "com.oracle.database.xml" % "xmlparserv2" % oraVersion % "test"
   * causes the following stacktrace; the validation in the oracle parser causes
   * issues when datanucleus is reading some xml file.
   * Stack Trace:
   * Caused by: MetaException(message:Error reading the Meta-Data input ... content model is not deterministic
      at oracle.xml.parser.v2.XMLError.flushErrorHandler(XMLError.java:425)
      at oracle.xml.parser.v2.XMLError.flushErrors1(XMLError.java:290)
      at oracle.xml.parser.v2.NonValidatingParser.parseContentModel(NonValidatingParser.java:982)
      at oracle.xml.parser.v2.NonValidatingParser.parseElementDecl(NonValidatingParser.java:960)
      at oracle.xml.parser.v2.NonValidatingParser.parseMarkupDecl(NonValidatingParser.java:854)
      at oracle.xml.parser.v2.NonValidatingParser.parseDoctypeDecl(NonValidatingParser.java:654)
      at oracle.xml.parser.v2.NonValidatingParser.parseProlog(NonValidatingParser.java:445)
      at oracle.xml.parser.v2.NonValidatingParser.parseDocument(NonValidatingParser.java:403)
      at oracle.xml.parser.v2.XMLParser.parse(XMLParser.java:244)
      at oracle.xml.jaxp.JXSAXParser.parse(JXSAXParser.java:298)
      at oracle.xml.jaxp.JXSAXParser.parse(JXSAXParser.java:236)
      at org.datanucleus.metadata.xml.MetaDataParser.parseMetaDataStream(MetaDataParser.java:283)
      at org.datanucleus.metadata.xml.MetaDataParser.parseMetaDataURL(MetaDataParser.java:144)
      at org.datanucleus.api.jdo.metadata.JDOMetaDataManager.parseFile(JDOMetaDataManager.java:263)
   */
  private def ensureSAXParserFactorySetPreferred: Unit = {
    /*
     * If 'javax.xml.parsers.SAXParserFactory' is not configured
     * - if 'oracle.xml.jaxp.JXSAXParserFactory' is on the classpath use it
     * - else use 'com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl'
     */
    if (System.getProperty("javax.xml.parsers.SAXParserFactory") == null) {
      val orclSAXParserFactoryClsName = "oracle.xml.jaxp.JXSAXParserFactory"
      val orclSAXFactAvail = Try {
        Utils.classForName("oracle.xml.jaxp.JXSAXParserFactory")
      }.toOption.isDefined

      System.setProperty(
        "javax.xml.parsers.SAXParserFactory",
        if (orclSAXFactAvail) orclSAXParserFactoryClsName
        else "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl"
      )
    }
  }

  /*
   * This is not ideal. Under VPN the
   * resolution of the xsd: http://java.sun.com/xml/ns/jdbc/webrowset.xsd
   * stalls for ~ 1 minute, before bailing.
   * So both reads and writes of xml files is very very slow.
   */
  private def ensureSAXParserFactorySet: Unit = {
    if (System.getProperty("javax.xml.parsers.SAXParserFactory") == null) {
      System.setProperty(
        "javax.xml.parsers.SAXParserFactory",
        "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl"
      )
    }
  }

  case class Instance(sql : String,
                      dsInfo : DataSourceInfo) {
    initCacheLoc(dsInfo.catalogOptions)

    private lazy val rsSetCacheFileNm: String = s"${dsInfo.key.hashCode}_${sql.hashCode}.xml"
    private lazy val resultSetCache: File = new File(cacheLoc, rsSetCacheFileNm)

    def saveResultSet(rs : ResultSet) : Unit = {
      val webRowSet = new OracleWebRowSet()
      try {
        webRowSet.writeXml(rs, new FileOutputStream(resultSetCache))
      } finally {
        webRowSet.close()
      }
    }

    def exists : Boolean = resultSetCache.exists()

    def loadCachedResultSet: ResultSet = {
      ensureSAXParserFactorySet
      val webRowSet = new OracleWebRowSet()
      /*
       * Observation on 11/10:
       * - relying on OracleWebRowSet has proven to be problematic
       *   - xml parsing issues, datatype support issues
       * - for now going to bail with an empty rowset if reading fails
       * - have to revisit how to support running tests that have result data
       */
      try {
        webRowSet.readXml(new FileInputStream(resultSetCache))
      } catch {
        case e : Exception => logError(
          s"""RETURNING EMPTY RESULTSET
             |Ignoring parse error for cached resultset file (${rsSetCacheFileNm})
             |SQL is:
             |${sql}""".stripMargin,
          e
        )
      }
      webRowSet.beforeFirst()
      webRowSet
    }
  }
}
