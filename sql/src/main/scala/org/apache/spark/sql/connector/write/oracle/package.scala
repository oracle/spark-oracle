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

package org.apache.spark.sql.connector.write

import org.apache.spark.sql.oracle.expressions.OraExpression

/**
 * There are 2 write paths:
 *
 *  - write of a Spark native Plan to an
 *    [[org.apache.spark.sql.connector.catalog.oracle.OracleTable]]
 *  - write of a [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation]]
 *    of a [[org.apache.spark.sql.connector.read.oracle.OraScan]]
 *
 * '''Write of a Spark Native Plan:'''
 *
 * These will be Spark Plans with the sink of the write being an `OracleTable`.
 * Spark Optimizer will setup such a plan in all cases.
 *
 *  - An ''physical rewrite rule'' we will add is to check if the input plan of such
 *    plan is a [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase]]
 *    on an `OraScan` we will rewrite this into the second case:
 *    ''Oracle DDL/DML command''.
 *
 * The one ''logical optimization'' we may do down the line is to
 * inject a shuffle when writing to a ''partitioned'' table; this
 * will ensure each task writes to few oracle partitions.
 *
 * The execution of such a plan is divided into 3 stages: Driver-side Init,
 * Executor-side task execution and Driver-side job finishing.
 * There are 3 kinds of writes done on the Oracle side to the destination table: APPEND rows,
 * UPDATE rows(done as delete existing ros + insert new rows) and partition exchange.
 * The assumption is that the oracle jdbc connection configured in Spark has the
 * privilege to create tables.
 *
 * The actions taken during the different modes are summarized below:
 *
 * <img src="doc-files/writeActionsBehavior.png" />
 *
 * ''Driver Side Setup flow:''
 *
 * <img src="doc-files/writePathDriverSetup.png" />
 *
 * ''Executor Side task execution:''
 *
 * <img src="doc-files/writePathExecutor.png" />
 *
 * ''Driver Side Finish flow:''
 *
 * <img src="doc-files/writePathDriverFinish.png" />
 *
 * '''Oracle DML/DDL command:'''
 *
 * - ''TBD''
 */
package object oracle {}
