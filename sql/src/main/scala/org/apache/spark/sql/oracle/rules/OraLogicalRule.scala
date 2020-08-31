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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.oracle.rules.sharding.{AnnotateCoordinatorCost, AnnotateOraPlan, AnnotateShardingInfoRule, RewriteAsShardPlan}

abstract class OraLogicalRule extends Rule[LogicalPlan] {

  protected def isRewriteEnabled(implicit sparkSession : SparkSession) : Boolean = {
    import org.apache.spark.sql.oracle.OraSparkConfig._
    getConf(ENABLE_ORA_PUSHDOWN)(sparkSession)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {

    implicit val sparkSession = OraSparkUtils.currentSparkSession

    if (isRewriteEnabled) {
      _apply(plan)
    } else plan
  }

  def _apply(plan: LogicalPlan)(implicit sparkSession : SparkSession): LogicalPlan

}

object OraLogicalRules extends OraLogicalRule {

  val RULES = Seq(
    OraSQLPushdownRule, OraFixColumnNames,
    AnnotateShardingInfoRule, AnnotateCoordinatorCost,
    RewriteAsShardPlan,
    AnnotateOraPlan
  )

  private val ORA_PUSHDOWN_TAG = TreeNodeTag[Boolean]("_OraPushdownApplied")

  private def pushdownApplied(plan : LogicalPlan) : Boolean =
    plan.getTagValue(ORA_PUSHDOWN_TAG).getOrElse(false)

  private def setPushDownApplied(plan : LogicalPlan) : Unit =
    plan.setTagValue(ORA_PUSHDOWN_TAG, true)

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan
  = plan match {
    /*
      [[Subquery]] is a marker operator to signal the child Plan is being
      optimized during subQuery analysis.
      Don't apply OraPushdown during this phase. We will apply
      pushdown after all subquery rewrites are applied.
    */
    case sq : Subquery => sq
    case p if !pushdownApplied(p) =>
      val r = RULES.foldLeft(p) {
        case (plan, r) => r(plan)
      }
      setPushDownApplied(r)
      r
    case p => p
  }
}
