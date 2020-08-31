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

import java.sql
import java.sql.{PreparedStatement, ResultSet, SQLException, Types}

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

object ORAMetadataSQLs {

  import ORASQLUtils._

  private val TABLE_METADATA_SQL = s"""
         declare 
           r_xml clob;
           r_sxml clob;
         begin
           SELECT "SYS"."DBMS_METADATA"."GET_XML" ('TABLE', ?, ?) into r_xml from dual;
           SELECT "SYS"."DBMS_METADATA"."GET_SXML" ('TABLE', ?, ?) into r_sxml from dual;
           ? := r_xml;
           ? := r_sxml;
         end;""".stripMargin

  private val TYP_METADATA_SQL = s"""
         declare
           r_sxml clob;
         begin
           SELECT "SYS"."DBMS_METADATA"."GET_SXML" ('TYPE', ?, ?) into r_sxml from dual;
           ? := r_sxml;
         end;""".stripMargin

  /**
   * Return the DBMS_METADATA.get_xml and DBMS_METADATA.get_sxml
   * output for the given table.
   *
   * @param dsKey
   * @param schema
   * @param table
   * @return
   */
  def tableMetadata(dsKey: DataSourceKey, schema: String, table: String): (String, String) = {
    var xml: String = null
    var sxml: String = null
    performDSCall(dsKey, TABLE_METADATA_SQL, s"get table metadata for ${schema}.${table}", { cs =>
      cs.setString(1, table)
      cs.setString(2, schema)
      cs.setString(3, table)
      cs.setString(4, schema)
      cs.registerOutParameter(5, Types.CLOB)
      cs.registerOutParameter(6, Types.CLOB)
    }, { cs =>
      xml = cs.getString(5)
      sxml = cs.getString(6)
    })
    (xml, sxml)
  }

  def tableExists(dsKey: DataSourceKey,
                 schemaO : Option[String],
                 table : String,
                 actionDetails : => String = null.asInstanceOf[String]): Boolean = {
    val schemaPrefix = schemaO.map(_ + ".").getOrElse("")
    perform[Boolean](
      dsKey,
      Option(actionDetails).
        getOrElse(s"check table(${schemaPrefix}${table}) exists")) { conn =>
      val meta = conn.getMetaData
      var res: ResultSet = null
      try {
        res = meta.getTables(dsKey.userName, schemaO.orNull, table, null)
        res.next()
      } finally {
        if (res != null) {
          res.close()
        }
      }
    }
  }

  @throws[sql.SQLException]
  def validateConnection(dsKey: DataSourceKey): Unit = {
    val plan_table_exists : Boolean =
    tableExists(dsKey, None, "PLAN_TABLE", "check instance is setup for spark-oracle")

    if (!plan_table_exists) {
      throw new SQLException(s"${dsKey} is not setup for spark-oracle: missing PLAN_TABLE")
    }
  }

  def listAccessibleUsers(dsKey: DataSourceKey): Array[String] = {
    performDSQuery(
      dsKey,
      """
        |select "USERNAME", "ALL_SHARD"
        |from "SYS"."ALL_USERS"
        |where "ORACLE_MAINTAINED" = 'N'
        |""".stripMargin,
      "list non oracle maintained users") { rs =>
      val buf = ArrayBuffer[String]()
      while (rs.next()) {
        buf += rs.getString(1)
      }
      buf.toArray
    }
  }

  def listAllTables(dsKey: DataSourceKey): Map[String, Array[String]] = {
    performDSQuery(
      dsKey,
      """
        |select "OWNER", "TABLE_NAME"
        |from "SYS"."ALL_TABLES"
        |where "OWNER" in (select "USERNAME" from "SYS"."ALL_USERS" where "ORACLE_MAINTAINED" = 'N')
        |order by "OWNER", "TABLE_NAME"
        |""".stripMargin,
      "list non oracle maintained users",
    ) { rs =>
      val m = MMap[String, Array[String]]()
      var currOwner: String = null
      var currBuf = ArrayBuffer[String]()

      while (rs.next()) {
        val nOwner = rs.getString(1)

        if (currOwner == null || currOwner != nOwner) {
          if (currBuf.nonEmpty) {
            m(currOwner) = currBuf.toArray
          }
          currOwner = nOwner
          currBuf = ArrayBuffer[String]()
        }
        currBuf += rs.getString(2)
      }

      if (currBuf.nonEmpty) {
        m(currOwner) = currBuf.toArray
      }

      m.toMap
    }
  }

  def queryPlan(dsKey: DataSourceKey,
                sql : String,
                setStmtParams: PreparedStatement => Unit = ps => ()) : String = {
    val stat_id = {
      val s = s"sp_ora_${System.currentTimeMillis()}"
      s.substring(0, Math.min(s.length(), 30))
    }
    val explainQuery =
      s"""explain plan
         |set statement_id = '${stat_id}'
         |for
         |${sql}""".stripMargin
    performDSQuery(
      dsKey,
      explainQuery,
      s"running explain for query, stat_id set to ${stat_id}",
      setStmtParams) { rs =>
      ()
    }

    val retrievePlanSQL =
    s"""select "SYS"."DBMS_XPLAN"."DISPLAY_PLAN"(
       |            statement_id => '${stat_id}', type => 'XML'
       |          ) from dual""".stripMargin

    performDSQuery(
      dsKey,
      retrievePlanSQL,
      s"retriving plan for query, stat_id set to ${stat_id}",
      ) { rs =>
      rs.next()
      rs.getString(1)
    }
  }

  def typeMetadata(dsKey: DataSourceKey, schema: String, typNm: String): String = {
    var sxml: String = null
    performDSCall(dsKey, TYP_METADATA_SQL, s"get table metadata for ${schema}.${typNm}", { cs =>
      cs.setString(1, typNm)
      cs.setString(2, schema)
      cs.registerOutParameter(3, Types.CLOB)
    }, { cs =>
      sxml = cs.getString(3)
    })
    sxml
  }

}
