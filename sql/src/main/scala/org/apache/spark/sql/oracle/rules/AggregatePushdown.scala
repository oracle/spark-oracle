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
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions.OraExpressions
import org.apache.spark.sql.oracle.operators.OraQueryBlock

case class AggregatePushdown(inDSScan: DataSourceV2ScanRelation,
                             inOraScan: OraScan,
                             inQBlk: OraQueryBlock,
                             pushdownCatalystOp: Aggregate,
                             sparkSession: SparkSession
                            )  extends OraPushdown with ProjectListPushdownHelper {

  private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    if (currQBlk.canApply(pushdownCatalystOp)) {
      val aggOp = pushdownCatalystOp
      val pushdownProjList =
        buildCleanedProjectList(aggOp.aggregateExpressions, currQBlk.catalystProjectList)
      for(
        oraGrpExpressions <- OraExpressions.unapplySeq(aggOp.groupingExpressions);
        oraSelExpressions <- OraExpressions.unapplySeq(pushdownProjList)
      ) yield {
        currQBlk.copyBlock(
          select = oraSelExpressions,
          groupBy = Some(oraGrpExpressions),
          catalystOp = Some(aggOp),
          catalystProjectList = aggOp.aggregateExpressions)
      }
    } else None
  }
}
