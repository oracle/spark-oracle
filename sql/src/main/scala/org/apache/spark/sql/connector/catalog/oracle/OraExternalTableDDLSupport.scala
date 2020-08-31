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

package org.apache.spark.sql.connector.catalog.oracle

import java.util
import java.util.Locale

import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{IdentityTransform, Transform}
import org.apache.spark.sql.types.{StructField, StructType}

trait OraExternalTableDDLSupport { self: OracleCatalog =>

  class OraCreateDDLBuilder(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]) {

    val oci_credential = getMetadataManager.catalogOptions.oci_credential_name

    lazy val location: Option[String] = Option(properties.get(TableCatalog.PROP_LOCATION))
    lazy val provider: Option[String] = Option(properties.get(TableCatalog.PROP_PROVIDER))

    lazy val (isValidProvider: Boolean, validProvider: String) =
      if (provider.isDefined) {
        provider.get.toLowerCase(Locale.ROOT) match {
          case "parquet" => (true, "parquet")
          case _ => (null, false)
        }
      } else (null, false)

    lazy val (isValidLocation: Boolean, validLocation: String) =
      if (location.isDefined) {
        val l = location.get
        if (l.startsWith("https://objectstorage")) {
          (true, l)
        } else {
          (false, "")
        }
      } else (false, "")

    lazy val isPartitioned = partitions.length > 0

    /**
     * Only single level identity partitions for now.
     */
    lazy val validPartitioningScheme =
      partitions.length <= 1 &&
        partitions.forall(p => p.isInstanceOf[IdentityTransform])

    lazy val isSupported: Boolean = oci_credential.isDefined &&
      isValidProvider &&
      isValidLocation &&
      validPartitioningScheme

    private def oraDataType(sField: StructField): (String, OraDataType) = ???

    private def buildColumnListParam: String = {
      val oraColumns = schema.fields.map(f => oraDataType(f))
      oraColumns
        .map {
          case (nm, oraType) => s"${nm} ${oraType.sqlType}" // FIXME to be sql
        }
        .mkString(", ")
    }

    private def buildFileURIListParam: String = ???

    private def buildFormatParam: String = "json_object('type' value 'parquet')"

    /**
     * Output is form partition_column value and partition location uri
     * @return
     */
    private def introspectPartitions: Array[(String, String)] = ???

    private def buildPartitioningParam: String = {
      val pCol = partitions.head.asInstanceOf[IdentityTransform].ref.fieldNames().mkString(".")

      val partitionSpecs =
        for (((pVal, pLoc), i) <- introspectPartitions.zipWithIndex) yield {
          s"partition p${i} values (${pVal}) location ${pLoc}"
        }

      s"""partition by list(${pCol})
         |(
         |${partitionSpecs.mkString(",\n")}
         |)""".stripMargin
    }

    private[oracle] def buildDDL: String = {
      if (!isPartitioned) {
        OraExternalTableDDLSupport.nonPartitionedTableDDL(
          ident.toString,
          oci_credential.get,
          buildFormatParam,
          buildColumnListParam,
          buildFileURIListParam)
      } else {
        OraExternalTableDDLSupport.partitionedTableDDL(
          ident.toString,
          oci_credential.get,
          buildFormatParam,
          buildColumnListParam,
          buildPartitioningParam)
      }
    }
  }

}

object OraExternalTableDDLSupport {

  private def partitionedTableDDL(
      tblNm: String,
      credName: String,
      formatClause: String,
      colListClause: String,
      partListClause: String) =
    s"""
       |BEGIN  
       |   DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE(
       |      table_name =>'${tblNm}',
       |      credential_name =>'${credName}',
       |      format => ${formatClause},
       |      column_list => '${colListClause}',
       |      partitioning_clause => '${partListClause}'
       |     );
       |   END;
       |""".stripMargin

  private def nonPartitionedTableDDL(
      tblNm: String,
      credName: String,
      formatClause: String,
      colListClause: String,
      fileURIClause: String) =
    s"""
       |BEGIN  
       |   DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE(
       |      table_name =>'${tblNm}',
       |      credential_name =>'${credName}',
       |      file_uri_list => '${fileURIClause}',
       |      format => ${formatClause},
       |      column_list => '${colListClause}'
       |     );
       |   END;
       |""".stripMargin

}
