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

package org.apache.spark.sql.connector.read.oracle

import oracle.spark.DataSourceKey

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.oracle.operators.OraPlan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/*
 * OraScanBuilder only used to capture building an OraScan from an OracleTable
 * - not needed during collpasing of Spark Plan
 * - so no need to have a OraPlan
 */
case class OraScanBuilder(
    sparkSession: SparkSession,
    dsKey: DataSourceKey,
    tblId: Identifier,
    table: OraTable,
    options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters {

  private val partitionSchema = table.partitionSchema

  private val partitionNameSet: Set[String] = partitionSchema.fields.map(_.name).toSet

  override def build(): Scan = {
    val readSchema = readDataSchema()
    val partitionSchema = readPartitionSchema()

    val oraPlan = OraPlan.buildOraPlan(
      table,
      readSchema.toAttributes ++ partitionSchema.toAttributes,
      pushedFilters())

    OraFileScan(
      sparkSession,
      table.catalystSchema,
      readSchema,
      partitionSchema,
      dsKey,
      oraPlan,
      options,
      Seq.empty,
      Seq.empty)
  }

  private var requiredSchema = table.catalystSchema

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  private def createRequiredNameSet(): Set[String] = requiredSchema.fields.map(_.name).toSet

  private def readDataSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val schema = requiredSchema
    val fields = schema.fields.filter { field =>
      val colName = table.columnNameMap(field.name)
      requiredNameSet.contains(colName) && !partitionNameSet.contains(colName)
    }
    StructType(fields)
  }

  private def readPartitionSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = partitionSchema.fields.filter { field =>
      val colName = table.columnNameMap(field.name)
      requiredNameSet.contains(colName)
    }
    StructType(fields)
  }

  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    _pushedFilters = filters
    filters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters
}
