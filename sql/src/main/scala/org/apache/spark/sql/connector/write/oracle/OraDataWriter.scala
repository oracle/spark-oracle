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

package org.apache.spark.sql.connector.write.oracle

import oracle.spark.DataSourceInfo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraColumn
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.oracle.expressions.{JDBCGetSet, OraLiterals}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}

case class OraDataWriterFactory(dsInfo: DataSourceInfo,
                                dataSchema : StructType,
                                oraTableShape : Array[OraColumn],
                                tempTableInsertSQL: String,
                                accumulators : OraDataWriter.OraInsertAccumulators
                               ) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    OraDataWriter(partitionId, taskId, dsInfo, dataSchema, oraTableShape,
      tempTableInsertSQL, accumulators)
}

case class OraDataWriter(partitionId: Int,
                         taskId: Long,
                         dsInfo: DataSourceInfo,
                         dataSchema : StructType,
                         oraTableShape : Array[OraColumn],
                         tempTableInsertSQL: String,
                         accumulators : OraDataWriter.OraInsertAccumulators)
    extends DataWriter[InternalRow] {

  import OraLiterals._

  private[this] lazy val oraInsert = OraInsertStatement(dsInfo, tempTableInsertSQL, accumulators)

  private[this] lazy val setters: IndexedSeq[JDBCGetSet[_]] =
    dataSchema.fields.map { attr => jdbcGetSet(attr.dataType)}

  override def write(record: InternalRow): Unit = {
    for (i <- (0 until setters.size)) {
      setters(i).setValue(record, oraInsert.underlying, i, oraTableShape)
    }
    oraInsert.addBatch
  }

  override def commit(): WriterCommitMessage = {
    oraInsert.finish
    oraInsert.commit()
    OraDataWriter.OraWriteCommitMessage
  }

  override def abort(): Unit = oraInsert.abort()

  override def close(): Unit = oraInsert.close()
}

object OraDataWriter {

  case class OraInsertAccumulators(rowsWritten: LongAccumulator, timeToWriteRows: DoubleAccumulator)

  private val ROW_WRITE_ACCUM_NAME = "oracle.query.rows_written"
  private val TIME_TO_WRITE_ROWS = "oracle.query.time_to_write_rows_msecs"

  def createAccumulators(sparkSession: SparkSession): OraInsertAccumulators = {
    val rw = sparkSession.sparkContext.longAccumulator(ROW_WRITE_ACCUM_NAME)
    val ttwrs = sparkSession.sparkContext.doubleAccumulator(TIME_TO_WRITE_ROWS)
    OraInsertAccumulators(rw, ttwrs)
  }

  case object OraWriteCommitMessage extends WriterCommitMessage
}