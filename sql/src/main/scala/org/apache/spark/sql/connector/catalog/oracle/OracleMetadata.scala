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

import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable.{ArrayBuffer, Stack}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, Transform}
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.types.{StructField, StructType}

object OracleMetadata extends OraFunctionDefs {

  trait OracleMetadataException extends AnalysisException

  class UnsupportedOraDataType(typeNm: String, reason: Option[String])
      extends AnalysisException(
        s"Unsupported Datatype ${typeNm}" +
          s" ${if (reason.isDefined) "," + reason.get else ""}")

  def unsupportedOraDataType(typeNm: String): Nothing =
    throw new UnsupportedOraDataType(typeNm, None)

  def checkDataType(cond: Boolean, typeNm: => String, reason: => String): Unit = {
    if (cond) {
      throw new UnsupportedOraDataType(typeNm, Some(reason))
    }
  }

  class UnsupportedAction(action: String, alternate: Option[String])
      extends AnalysisException(
        s"Unsupported Action on Oracle Catalog: ${action}" +
          s"${if (alternate.isDefined) "\n " + alternate.get else ""}")

  def unsupportedAction(
      action: String,
      alternate: Option[String] = Some("you should perform this using Oracle SQL")): Nothing =
    throw new UnsupportedAction(action, alternate)

  class InvalidAction(action: String, suggestRemedies: Option[String])
    extends AnalysisException(
      s"Invalid Action on Oracle Catalog: ${action}" +
        s"${if (suggestRemedies.isDefined) "\n " + suggestRemedies.get else ""}")

  def invalidAction(action: String,
                    suggestRemedies: Option[String]): Nothing =
    throw new InvalidAction(action, suggestRemedies)

  object OraPartitionType extends Enumeration {
    val RANGE = Value("RANGE")
    val LIST = Value("LIST")
    val HASH = Value("HASH")

    def apply(s: String): Value = s match {
      case "RANGE_PARTITIONING" => RANGE
      case "RANGE_SUBPARTITIONING" => RANGE
      case "LIST_PARTITIONING" => LIST
      case "LIST_SUBPARTITIONING" => LIST
      case "HASH_PARTITIONING" => HASH
      case "HASH_SUBPARTITIONING" => HASH
      case _ => OraSparkUtils.throwAnalysisException(s"Unsupported Partition type '${s}'")
    }
  }

  case class OraIdentifier(namespace: Seq[String], name: String)

  case class OraColumn(
      name: String,
      dataType: OraDataType,
      collateName: Option[String],
      isNotNull: Boolean) {
    def dump(buf: StringBuilder): Unit = {
      buf.append(s"  Column : name=${name}, type=${dataType},")
      if (collateName.isDefined) {
        buf.append(s"collateName=${collateName}, ")
      }
      buf.append(s"isNotNull=${isNotNull}\n")
    }
  }

  case class OraPrimaryKey(cols: Array[String]) {
    def dump(buf: StringBuilder): Unit = {
      buf.append(s"  Primary Key columns: [${cols.mkString(",")}]")
    }
  }

  case class OraForeignKey(
      cols: Array[String],
      referencedTable: (String, String),
      referencedCols: Array[String]) {
    def dump(buf: StringBuilder): Unit = {
      buf.append(
        s"  Foreign Key : columns=[${cols.mkString(",")}], " +
          s"referencedTable=${referencedTable}, " +
          s"referencedColumns=[${referencedCols.mkString(",")}]\n")
    }
  }

  case class OraTablePartition(
      name: String,
      idx: Int,
      values: String,
      subPartitions: Array[OraTablePartition]) {

    def dump(buf: StringBuilder): Unit = {
      buf.append(s"Partition: name=${name}, values=${values}\n")
      for (sP <- subPartitions) {
        buf.append(s"Sub-Partition: name=${sP.name}, values=${sP.values}\n")
      }
    }
  }

  case class TablePartitionScheme(
      columns: Array[String],
      partType: OraPartitionType.Value,
      subPartitionScheme: Option[TablePartitionScheme]) {

    def dump(buf: StringBuilder): Unit = {
      buf.append(s"Partition Scheme: type=${partType}, columns=[${columns.mkString(",")}]\n")
      if (subPartitionScheme.isDefined) {
        val sP = subPartitionScheme.get
        buf.append(
          s"Sub-Partition Scheme: type=${sP.partType}, " +
            s"columns=[${sP.columns.mkString(",")}]\n")
      }
    }

    @transient lazy val transforms: Array[Transform] = {
      import LogicalExpressions._
      val arr = ArrayBuffer[Transform]()

      def addTransforms(columns: Array[String]) =
        columns.foreach(c => arr += identity(parseReference(c)))

      addTransforms(columns)
      if (subPartitionScheme.isDefined) {
        addTransforms(subPartitionScheme.get.columns)
      }
      arr.toArray
    }
  }

  case class TableStats(
      num_blocks: Option[Long],
      block_size: Option[Int],
      row_count: Option[Long],
      avg_row_size_bytes: Option[Double]) {

    def dump(buf: StringBuilder): Unit = {
      buf.append(s"  Stats: ")
      if (num_blocks.isDefined) {
        buf.append(s"num_blocks = ${num_blocks.get} ")
      }
      if (block_size.isDefined) {
        buf.append(s"block_size = ${block_size.get} ")
      }
      if (row_count.isDefined) {
        buf.append(s"row_count = ${row_count.get} ")
      }
      if (avg_row_size_bytes.isDefined) {
        buf.append(s"avg_row_size_bytes = ${avg_row_size_bytes.get} ")
      }
      buf.append("\n")
    }
  }

  @SerialVersionUID(-7332993419881053598L)
  case class OraTable(
      schema: String,
      name: String,
      columns: Array[OraColumn],
      partitionScheme: Option[TablePartitionScheme],
      partitions: Array[OraTablePartition],
      primaryKey: Option[OraPrimaryKey],
      foreignKeys: Array[OraForeignKey],
      is_external: Boolean,
      tabStats: TableStats,
      properties: Map[String, String]) {

    def dump(buf: StringBuilder): Unit = {
      buf.append(s"Table: schema=${schema}, name=${name}, isExternal=${is_external}\n")
      for (c <- columns) {
        c.dump(buf)
      }
      for (pS <- partitionScheme) {
        pS.dump(buf)
      }
      for (p <- partitions) {
        p.dump(buf)
      }
      for (pk <- primaryKey) {
        pk.dump(buf)
      }
      for (fk <- foreignKeys) {
        fk.dump(buf)
      }
      tabStats.dump(buf)
      buf.append(s"  Properties: ${properties}")
    }

    private def isPartitionColumn(colNm : String) : Boolean = {

      def isPartColumn(pS : TablePartitionScheme,
                       colNm : String) : Boolean =
        pS.columns.contains(colNm) ||
          pS.subPartitionScheme.map(isPartColumn(_, colNm)).getOrElse(false)

      partitionScheme.map(isPartColumn(_, colNm)).getOrElse(false)
    }

    @transient lazy val columnNameMap: CaseInsensitiveMap[String] =
      CaseInsensitiveMap(columns.map(c => c.name -> c.name).toMap)

    @transient lazy val catalystSchema: StructType =
      StructType(columns.map(c => StructField(c.name, c.dataType.catalystType, !c.isNotNull)))

    def isPartitioned: Boolean = partitionScheme.isDefined

    @transient lazy val (dataSchema: StructType, partitionSchema: StructType) = {
      val partCols = columns.filter(c => isPartitionColumn(c.name)).map(_.name)
        // partitionScheme.map(_.columns.toSet).getOrElse(Set.empty)
      (
        StructType(catalystSchema.fields.filterNot(f => partCols.contains(f.name))),
        StructType(catalystSchema.fields.filter(f => partCols.contains(f.name))))
    }

    @transient private lazy val partitionNameIdxSeq : Seq[String] = {
      val ab = ArrayBuffer[String]()
      val s = Stack[OraTablePartition]()
      s.pushAll(partitions.reverse)
      while(s.nonEmpty) {
        val p = s.pop()
        if (p.subPartitions.nonEmpty) {
          s.pushAll(p.subPartitions.reverse)
        } else {
          ab += p.name
        }
      }
      ab.toIndexedSeq
    }

    @transient lazy val  isSubPartitioned : Boolean =
      partitions.exists(p => p.subPartitions.nonEmpty)

    /**
     * Numbering starts at 1. Return partitions in the subrange(s,e)
     * both ends are inclusive.
     *
     * @param start
     * @param end
     * @return
     */
    def partitions(start : Int, end : Int) : Seq[String] = {
      partitionNameIdxSeq.slice(start-1, `end`)
    }

    def allPartitions : Seq[String] = partitionNameIdxSeq

    private[oracle] def showPartitions : Array[Seq[OraTablePartition]] = {

      /**
       * each output entry represents a partition-subPartition-... path
       * return all possible suhc paths under this partition
       * @return
       */
      def showPartitions(oraP : OraTablePartition) : Seq[Seq[OraTablePartition]] = {
        if (oraP.subPartitions.isEmpty) {
          Seq(Seq(oraP))
        } else {
          for (
            sP <- oraP.subPartitions.toSeq;
            sPE : Seq[OraTablePartition] <- showPartitions(sP)
          ) yield {
            oraP +: sPE
          }
        }
      }


      partitions.map(showPartitions).flatten
    }

    private[oracle] def showPartitionScheme : Seq[TablePartitionScheme] = {

      def showPartitionScheme(tS : TablePartitionScheme) : Seq[TablePartitionScheme] = {
        Seq(tS) ++ tS.subPartitionScheme.toSeq.flatMap(showPartitionScheme)
      }

      partitionScheme.toSeq.flatMap(showPartitionScheme)
    }
  }

  private[oracle] val NAMESPACES_CACHE_KEY = "__namespaces__".getBytes(UTF_8)
  private[oracle] val TABLE_LIST_CACHE_KEY = "__tables_list__".getBytes(UTF_8)

}
