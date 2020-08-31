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

import oracle.spark.ConnectionManagement

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.oracle.OraSparkUtils

case class OraBatchWrite(writeSpec: OraWriteSpec) extends BatchWrite {

  lazy val oraWriteActions = OraWriteActions(writeSpec)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    oraWriteActions.createTempTable
    val dsInfo = ConnectionManagement.info(writeSpec.dsKey)
    val accumulators = OraDataWriter.createAccumulators(OraSparkUtils.currentSparkSession)

    OraDataWriterFactory(
      dsInfo,
      writeSpec.oraTable.catalystSchema,
      writeSpec.oraTableShape,
      oraWriteActions.insertTempTableDML,
      accumulators
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    try {
      oraWriteActions.prepareForUpdate
      oraWriteActions.updateDestTable
      oraWriteActions.dropTempTable
    } finally {
      oraWriteActions.close
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    try {
      oraWriteActions.dropTempTable
    } finally {
      oraWriteActions.close
    }
  }

  override def useCommitCoordinator(): Boolean = true

  override def onDataWriterCommit(message: WriterCommitMessage): Unit = ()
}
