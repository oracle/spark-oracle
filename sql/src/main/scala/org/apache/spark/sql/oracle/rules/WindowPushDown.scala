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

package org.apache.spark.sql.oracle.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions.OraExpressions
import org.apache.spark.sql.oracle.operators.OraQueryBlock

/**
 *  - the [[ExtractWindowExpressions]] Spark rule breaksdown Window Expressions in a Query Block
 *    into a sequence of Window Operators, each of which contains
 *    [[org.apache.spark.sql.catalyst.expressions.WindowExpression]]
 *    with the same `partition` and `order` specification.
 *  - the [[ResolveWindowFrame]] Spark rule ensures the validity of
 *    [[org.apache.spark.sql.catalyst.expressions.WindowFrame]]s in
 *    [[org.apache.spark.sql.catalyst.expressions.WindowExpression]]s.
 *  - [[Window]] operators project all their child output `+`
 *    [[Window.windowExpressions]] defined.
 *
 * @param inDSScan
 * @param inOraScan
 * @param inQBlk
 * @param pushdownCatalystOp
 * @param sparkSession
 */
case class WindowPushDown(inDSScan: DataSourceV2ScanRelation,
                          inOraScan: OraScan,
                          inQBlk: OraQueryBlock,
                          pushdownCatalystOp: Window,
                          sparkSession: SparkSession
                          ) extends OraPushdown with ProjectListPushdownHelper {


  override private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    if (currQBlk.canApply(pushdownCatalystOp)) {
      val windowOp = pushdownCatalystOp
      val pushdownProjList =
        buildCleanedProjectList(windowOp.windowExpressions, currQBlk.catalystProjectList)
      for(
        oraSelExpressions <- OraExpressions.unapplySeq(pushdownProjList)
      ) yield {
        currQBlk.copyBlock(
          select = currQBlk.select ++ oraSelExpressions,
          catalystProjectList = currQBlk.catalystProjectList ++ windowOp.windowExpressions)
      }
    } else {
      None
    }
  }
}
