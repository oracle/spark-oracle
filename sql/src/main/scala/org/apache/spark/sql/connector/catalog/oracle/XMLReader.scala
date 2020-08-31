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

import scala.xml.{Node, NodeSeq}

import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.{
  OraColumn,
  OraForeignKey,
  OraPartitionType,
  OraPrimaryKey,
  OraTable,
  OraTablePartition,
  TablePartitionScheme,
  TableStats
}

object XMLParsingUtils {

  val NAME_TAG = "NAME"
  val SCHEMA_TAG = "SCHEMA"

  private[oracle] def textValue(nd: NodeSeq): Option[String] = {
    if (nd.nonEmpty) Some(nd.text) else None
  }

  private[oracle] def intValue(nd: NodeSeq): Option[Int] = {
    textValue(nd).map(_.toInt)
  }

  private[oracle] def longValue(nd: NodeSeq): Option[Long] = {
    textValue(nd).map(_.toLong)
  }

  private[oracle] def doubleValue(nd: NodeSeq): Option[Double] = {
    textValue(nd).map(_.toDouble)
  }

  private[oracle] def exists(nd: NodeSeq): Boolean = nd.nonEmpty

  private[oracle] def columnNames(nd: NodeSeq): Array[String] =
    (nd \ "COL_LIST" \ "COL_LIST_ITEM" \ NAME_TAG).map(_.text).toArray

}

trait PartitionParsing { self: XMLReader.type =>

  import XMLParsingUtils._

  private[oracle] case class PartitioningCls(partNode: NodeSeq) {
    val partitionNodeSeq: NodeSeq = partNode \ "PARTITION_LIST" \ "PARTITION_LIST_ITEM"
    val physicalPropertyNode: NodeSeq = partNode \ "DEFAULT_PHYSICAL_PROPERTIES"

    def tablePartition(nd: Node, idx: Int): OraTablePartition = {

      val pNm = (nd \ NAME_TAG).text
      val values = (nd \ "VALUES").text
      val subParts =
        for ((sNd, sIdx) <- (nd \ "SUBPARTITION_LIST" \ "SUBPARTITION_LIST_ITEM").zipWithIndex)
          yield {
            tablePartition(sNd, sIdx)
          }
      OraTablePartition(pNm, idx, values, subParts.toArray)
    }

    lazy val partitions: Array[OraTablePartition] =
      (for ((p, i) <- partitionNodeSeq.zipWithIndex) yield {
        tablePartition(p, i)
      }).toArray

    lazy val partitionScheme: TablePartitionScheme = {

      val cols = columnNames(partNode)
      val partType = OraPartitionType(partNode.apply(0).label)

      val rng_subPart = partNode \ "RANGE_SUBPARTITIONING"
      val list_subPart = partNode \ "LIST_SUBPARTITIONING"
      val hash_subPart = partNode \ "HASH_SUBPARTITIONING"

      var subParScheme: Option[TablePartitionScheme] = None

      if (rng_subPart.nonEmpty) {
        val subCols = columnNames(rng_subPart)
        subParScheme = Some(TablePartitionScheme(subCols, OraPartitionType.RANGE, None))
      } else if (list_subPart.nonEmpty) {
        val subCols = columnNames(list_subPart)
        subParScheme = Some(TablePartitionScheme(subCols, OraPartitionType.LIST, None))
      } else if (hash_subPart.nonEmpty) {
        val subCols = columnNames(hash_subPart)
        subParScheme = Some(TablePartitionScheme(subCols, OraPartitionType.HASH, None))
      }

      TablePartitionScheme(cols, partType, subParScheme)
    }
  }
}

trait TableSXMLParsing { self: XMLReader.type =>

  import XMLParsingUtils._

  private[oracle] case class TableProperties(nd: NodeSeq) {
    lazy val partitioning: Option[PartitioningCls] = {
      if (!(nd \ "RANGE_PARTITIONING").isEmpty) {
        Some(PartitioningCls(nd \ "RANGE_PARTITIONING"))
      } else if (!(nd \ "LIST_PARTITIONING").isEmpty) {
        Some(PartitioningCls(nd \ "LIST_PARTITIONING"))
      } else if (!(nd \ "HASH_PARTITIONING").isEmpty) {
        Some(PartitioningCls(nd \ "HASH_PARTITIONING"))
      } else {
        None
      }
    }
  }

  private[oracle] case class TableSchema(nd: NodeSeq) {
    lazy val name = (nd \ NAME_TAG).text
    lazy val datatype = textValue(nd \ "DATATYPE").get
    lazy val collateName = textValue(nd \ "COLLATE_NAME")
    lazy val length = intValue((nd \ "LENGTH"))
    lazy val precision = intValue(nd \ "PRECISION")
    lazy val scale = intValue(nd \ "SCALE")
    lazy val notNULL = exists(nd \ "NOT_NULL")
    val typProps = exists(nd \ "TYPE_PROPERTIES")
    val udtOwner = if (typProps) textValue(nd \ "TYPE_PROPERTIES" \ "SCHEMA") else None
    val udtTypNm = if (typProps) textValue(nd \ "TYPE_PROPERTIES" \ "NAME") else None
    val oraTypName = OraDataType.dataTypeName(datatype.toUpperCase(), udtOwner, udtTypNm)
  }

  private[oracle] def table_sxml(nd: NodeSeq): TableSXML =
    TableSXML(nd \ "RELATIONAL_TABLE", (nd \ NAME_TAG).text, (nd \ SCHEMA_TAG).text)

  private[oracle] case class TableSXML(nd: NodeSeq, name: String, schema: String) {

    private def column(nd: Node): OraColumn = {
      val tc = TableSchema(nd)
      OraColumn(
        tc.name,
        OraDataType.create(tc.oraTypName, tc.length, tc.precision, tc.scale),
        tc.collateName,
        tc.notNULL)
    }

    lazy val columns: Array[OraColumn] = {
      val cols = nd \ "COL_LIST" \ "COL_LIST_ITEM"
      cols.map(column).toArray
    }

    lazy val table_properties = TableProperties(nd \ "TABLE_PROPERTIES")

    private def partitioning = table_properties.partitioning

    def partitionScheme: Option[TablePartitionScheme] = partitioning.map(_.partitionScheme)

    def partitions: Array[OraTablePartition] =
      partitioning.map(_.partitions).getOrElse(Array.empty)

    lazy val primaryKey: Option[OraPrimaryKey] = {
      val primaryKeyNode = nd \ "PRIMARY_KEY_CONSTRAINT_LIST"
      if (primaryKeyNode.nonEmpty) {
        Some(
          OraPrimaryKey(
            primaryKeyNode
              .map(ns => columnNames(ns \ "PRIMARY_KEY_CONSTRAINT_LIST_ITEM"))
              .toArray
              .flatten))
      } else None
    }

    lazy val foreignKeys: Array[OraForeignKey] = {
      val foreignKeyNode = nd \ "FOREIGN_KEY_CONSTRAINT_LIST"
      def referencedTable(nd: NodeSeq): (String, String, Array[String]) = {
        // Pass the Schema and the Name and the Col List
        (
          (nd \ SCHEMA_TAG).text,
          (nd \ NAME_TAG).text,
          columnNames(nd))
      }

      (
        for (fk <- (foreignKeyNode \ "FOREIGN_KEY_CONSTRAINT_LIST_ITEM"))
          yield {
            val cols = columnNames(fk)
            val (fschema, fname, fcols) = referencedTable(fk \ "REFERENCES")
            OraForeignKey(cols, (fschema, fname), fcols)
          }
      ).toArray
    }

    lazy val physicalPropertyNode = (nd \ "PHYSICAL_PROPERTIES")

    lazy val externalNd: NodeSeq = {
      val propNd = partitioning.map(_.physicalPropertyNode).getOrElse(physicalPropertyNode)
      propNd \ "EXTERNAL_TABLE"
    }

    lazy val isExternal = externalNd.nonEmpty

    lazy val extTableProps: Map[String, String] = {
      var extProp: Map[String, String] = Map()

      if ((externalNd \ "ACCESS_DRIVER_TYPE").nonEmpty) {
        extProp += ("ACCESS_DRIVER_TYPE" -> (externalNd \ "ACCESS_DRIVER_TYPE").text)
      }
      if ((externalNd \ "DEFAULT_DIRECTORY").nonEmpty) {
        extProp += ("DEFAULT_DIRECTORY" -> (externalNd \ "DEFAULT_DIRECTORY").text)
      }
      if ((externalNd \ "ACCESS_PARAMETERS").nonEmpty) {
        extProp += ("ACCESS_PARAMETERS" -> (externalNd \ "ACCESS_PARAMETERS").text)
      }
      if ((externalNd \ "LOCATION").nonEmpty) {
        val locList =
          (externalNd \ "LOCATION" \ "LOCATION_ITEM" \ NAME_TAG).map(_.text).mkString(" : ")
        extProp += ("LOCATION" -> locList)
      }
      if ((externalNd \ "REJECT_LIMIT").nonEmpty) {
        extProp += ("REJECT_LIMIT" -> (externalNd \ "REJECT_LIMIT").text)
      }
      extProp
    }

    lazy val propertiesMap: Map[String, String] = extTableProps
  }
}

trait TableXMLParsing {
  self: XMLReader.type =>

  import XMLParsingUtils._

  private[oracle] def table_xml(nd: NodeSeq): TableXML =
    TableXML(nd \ "ROW" \ "TABLE_T")

  private[oracle] case class TableXML(tblNd: NodeSeq) {
    lazy val block_count = longValue(tblNd \ "BLKCNT")
    lazy val row_count = longValue(tblNd \ "ROWCNT")
    lazy val block_size = intValue(tblNd \ "BLOCKSIZE")
    lazy val avg_row_size = doubleValue(tblNd \ "AVGRLN")

    lazy val tStats = TableStats(block_count, block_size, row_count, avg_row_size)
  }

}

object XMLReader extends PartitionParsing with TableSXMLParsing with TableXMLParsing {

  import scala.xml.XML._

  def parseTable(xml: String, sxml: String): OraTable = {
    val tbl_sxml = table_sxml(loadString(sxml))

    val tblProps = tbl_sxml.propertiesMap
    val tblStats = table_xml(loadString(xml)).tStats

    OraTable(
      tbl_sxml.schema,
      tbl_sxml.name,
      tbl_sxml.columns,
      tbl_sxml.partitionScheme,
      tbl_sxml.partitions,
      tbl_sxml.primaryKey,
      tbl_sxml.foreignKeys,
      tbl_sxml.isExternal,
      tblStats,
      tblProps)
  }
}
