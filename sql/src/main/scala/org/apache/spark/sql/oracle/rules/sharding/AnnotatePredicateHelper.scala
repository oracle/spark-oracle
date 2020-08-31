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

package org.apache.spark.sql.oracle.rules.sharding

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.oracle.sharding.{ReplicatedQuery, ShardedQuery, ShardQueryInfo, ShardTable}
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

trait AnnotatePredicateHelper extends PredicateHelper {

  type OpCode = Int

  /**
   * Copied and altered from [[PredicateHelper.findExpressionAndTrackLineageDown()]]
   * - for DSV2Scan default behavior is to stop there because it is a LeafNode
   * - but we want to traverse down to the original Plan that it has rewritten
   *
   * @param exp
   * @param plan
   * @return
   */
  def _findExpressionAndTrackLineageDown(
                                          exp: Expression,
                                          plan: LogicalPlan): Option[(Expression, LogicalPlan)] = {

    plan match {
      case DataSourceV2ScanRelation(_, oScan: OraScan, _)
        if oScan.oraPlan.catalystOp.isDefined =>
        _findExpressionAndTrackLineageDown(exp, oScan.oraPlan.catalystOp.get)
      case p: Project =>
        val aliases = getAliasMap(p)
        _findExpressionAndTrackLineageDown(replaceAlias(exp, aliases), p.child)
      // we can unwrap only if there are row projections, and no aggregation operation
      case a: Aggregate =>
        val aliasMap = getAliasMap(a)
        _findExpressionAndTrackLineageDown(replaceAlias(exp, aliasMap), a.child)
      case l: LeafNode if exp.references.subsetOf(l.outputSet) =>
        Some((exp, l))
      case other =>
        other.children.flatMap {
          child => if (exp.references.subsetOf(child.outputSet)) {
            _findExpressionAndTrackLineageDown(exp, child)
          } else {
            None
          }
        }.headOption
    }
  }

  def toShardingKey(e: Expression,
                    plan : LogicalPlan
                   ): Option[(OpCode, ShardTable, Int)] = {
    for ((baseE, plan) <- _findExpressionAndTrackLineageDown(e, plan)
         if baseE.isInstanceOf[AttributeReference];
         aRef = baseE.asInstanceOf[AttributeReference];
         opCode = plan.hashCode();
         sInfo <- ShardQueryInfo.getShardingQueryInfo(plan)
         if sInfo.queryType == ShardedQuery && sInfo.shardTables.size == 1;
         shardTable = sInfo.shardTables.head;
         sCol <- shardTable.isShardingKey(aRef)
         ) yield {
      (opCode, shardTable, sCol)
    }
  }

  def isGroupingOnReplTable(e: Expression,
                            plan : LogicalPlan
                           ) : Boolean = {
    (for ((baseE, plan) <- _findExpressionAndTrackLineageDown(e, plan)
         if baseE.isInstanceOf[AttributeReference];
         aRef = baseE.asInstanceOf[AttributeReference];
         sInfo <- ShardQueryInfo.getShardingQueryInfo(plan)
         ) yield {
      sInfo.queryType == ReplicatedQuery
    }).getOrElse(false)
  }

}
