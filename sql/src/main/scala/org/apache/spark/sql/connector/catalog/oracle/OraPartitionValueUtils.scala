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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast, GenericInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.{OraPartitionType, OraTablePartition, TablePartitionScheme}
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

object OraPartitionValueUtils {

  private[oracle] def isMappableToSpark(oraPartScheme : Seq[TablePartitionScheme]) : Boolean =
    oraPartScheme.forall(s => s.columns.size == 1)

  private[oracle] def createNullRow(partSchema : StructType) : InternalRow = {
    val row = new GenericInternalRow(partSchema.size)
    for(i <- (0 until partSchema.size)) {
      row.setNullAt(i)
    }
    row
  }

  /**
   * Attempt to convert an oracle Partition Specification into a Spark PartitionSpec.
   * Oracle has many partitioning schemes like RANGE, LIST, INTERVAL, HASH,
   * where only the LIST partition scheme somewhat maps to Spark's notion of
   * Partition.
   * - A Range Partition represents a range of values.
   * - A List partition represents a set of distinct values. And the domain can be more than
   *   1 column. For example `PARTITION BY LIST (state, channel)`
   * - An interval Partition is a special kind of range partition.
   * - A hash partition represents all domain values that hash to the same value.
   *
   * Whereas in Spark a partition represents a single value of a single column.
   * So only in the case of List partitioning on single columns can we properly map a
   * partition specification to a Spark partition specification. In general we
   * will recommend user to invoke the extension statement `show oracle partitions...`.
   * But if they issue a spark `show partitions`:
   * - we will try to resolve the [[OraTablePartition.values]] string as a Spark
   *   [[org.apache.spark.sql.catalyst.expressions.Literal]].
   * - In many instances this value cannot be interpreted as a Spark Literal,
   *   so a null will be shown.
   *
   * @param oraPartSpec
   * @param partSchema
   * @return
   */
  private[oracle] def toInternalRow(oraPartSpec : Seq[OraTablePartition],
                    oraPartScheme : Seq[TablePartitionScheme],
                    partSchema : StructType,
                    nullRow : InternalRow,
                    partitionRowEvaluator: PartitionRowEvaluator
                   ) : InternalRow = {

    if (isMappableToSpark(oraPartScheme)) {
      partitionRowEvaluator(oraPartSpec)
    } else {
      nullRow
    }
  }

  private[oracle] case class PartitionRowEvaluator(partSchema : StructType) {
    val numCols = partSchema.size

    val sessTZ = OraSparkUtils.currentSparkSession.sqlContext.conf.sessionLocalTimeZone

    val iRow = new GenericInternalRow(new Array[Any](numCols))
    val projections = for (i <- (0 until numCols)) yield {
      Cast(BoundReference(i, StringType, true), partSchema(i).dataType).withTimeZone(sessTZ)
    }

    val oArray = new Array[Any](numCols)
    val oRow = new GenericInternalRow(oArray)
    val projection = GenerateMutableProjection.generate(projections).target(oRow)

    def apply(oraPartSpec : Seq[OraTablePartition]) : InternalRow = {
      for (i <- (0 until numCols)) {
        iRow(i) = UTF8String.fromString(oraPartSpec(i).values)
      }
      projection(iRow)
      new GenericInternalRow(oRow.values.clone())
    }
  }

  /**
   * Output a information String about the finest grain partition represented by  oraPartSpec
   * @param oraPartSpec
   * @param oraPartScheme
   * @return
   */
  private[oracle] def showOraPartitions(oraPartSpec : Seq[OraTablePartition],
                                        oraPartScheme : Seq[TablePartitionScheme]) : String = {

    val partSpecs = for (((p, s), i) <- oraPartSpec.zip(oraPartScheme).zipWithIndex) yield {
      val pTyp = if (i == 0) "Partition" else "Sub-Partition"
      val pKnd = s.partType.toString
      val pCols = s.columns.mkString("[", ", ", "]")
      val pVals = p.values
      val pNm = p.name
      val pIdx = p.idx
      s"${pIdx}. ${pTyp}(name=${pNm}, values=${pVals})"
    }

    partSpecs.mkString(", ")
  }

  private[oracle] def showOraPartitionScheme(oraPartScheme : Seq[TablePartitionScheme]) : String = {
    val partSpecs = for ((s, i) <- oraPartScheme.zipWithIndex) yield {
      val pTyp = if (i == 0) "Partition" else "Sub-Partition"
      val pKnd = s.partType.toString
      val pCols = s.columns.mkString("[", ", ", "]")
      s"${pTyp}(kind=${pKnd}, cols=${pCols})"
    }
    partSpecs.mkString("Structure: ", ", ", "")
  }

}
