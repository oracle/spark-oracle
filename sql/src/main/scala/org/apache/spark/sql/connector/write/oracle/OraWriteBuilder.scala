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

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.oracle.expressions.DataSourceFilterTranslate
import org.apache.spark.sql.sources.Filter

case class OraWriteBuilder(writeSpec: OraWriteSpec)
    extends WriteBuilder
    with SupportsTruncate
    with SupportsOverwrite
    with SupportsDynamicOverwrite {

  /*
   * Called in non-dynamic partition mode from OverwriteByExpressionExec
   * when deleteCond is not just `true`
   */
  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    val oraExpr =
      DataSourceFilterTranslate(filters, writeSpec.oraTable).oraExpression
    OraWriteBuilder(writeSpec.setDeleteFilters(oraExpr))
  }

  /*
   * Called in dynamic partition mode from OverwritePartitionsDynamicExec
   */
  override def overwriteDynamicPartitions(): WriteBuilder = {
    OraWriteBuilder(writeSpec.setDynPartitionOverwriteMode)
  }

  override def buildForBatch(): BatchWrite = OraBatchWrite(writeSpec)

  /*
   * Called in non-dynamic partition mode from OverwriteByExpressionExec
   * when deleteCond is just `true`
   */
  override def truncate(): WriteBuilder = {
    OraWriteBuilder(writeSpec.setTruncate)
  }

}
