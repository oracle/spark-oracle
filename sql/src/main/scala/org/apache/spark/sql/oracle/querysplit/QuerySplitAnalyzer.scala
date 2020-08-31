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

package org.apache.spark.sql.oracle.querysplit

import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LocalLimit, LogicalPlan, Project}
import org.apache.spark.sql.connector.read.oracle.{OraFileScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.operators.{OraPlan, OraSingleQueryBlock, OraTableScan}

/**
 * Encapsulates logic for analyzing an [[OraPlan]] and its associated [[LogicalPlan]]
 * and coming up with potential [[SplitCandidate]]s: which are
 * [[org.apache.spark.sql.oracle.operators.OraJoinClause]]s with associated [[OraTableScan]]s or
 * straight-up [[OraTableScan]]s on which [[OraPartitionSplit]] or [[OraRowIdSplit]] could be
 * applied. In general a 'non-trivial' pushdown query can only be split by [[OraResultSplit]]
 * strategy since semantically the result of the pushdown query in Spark is the union-all of the
 * output of the rdd partition of the bridge
 * [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation]].
 *
 * But for certain query patterns we can split on a table involved in the query by
 * [[OraPartitionSplit]] or [[OraRowIdSplit]].
 * Here are a few examples(we anticipate this list will grow over time):
 *  - A `select .. from tabRef where ...` involving only 1 table can be split on that table.
 *  - A `select .. from tabRef1, tRef2,... where ...` '''involving multiple tables inner joined'''
 *    can be split on any of the tables. But arbitrary choice of the split may lead to significant
 *    extra processing in the Oracle instance. A good choice is to choose the table that is
 *    significantly larger(say at least 10 times larger) than any of the other tables.
 *    This is an good indicator of a star-join, where we split on the 'fact' table.
 *    In case of ''outer joins'' the null-supplying side can be split.
 *  - Other split rules in the future could choose to split on the joining columns using
 *    `CREATE_CHUNKS_BY_NUMBER_COL` from the `dbms_parallel_execute` package.
 *  - An `Agg-Join` plan: `select gBy_exprs, aggExprs from tabRef1, tRef2,... where ...`
 *    where there exists groupExprs that refer to a column can be split on that column.
 *    For example `select cust_name, avg(sales) from sales join cust ... group by cust_name`
 *    can be split on `cust_name`. Further
 *    `select to_date(product_intro_date), cust_name, avg(sales) from sales join cust join product ...`
 *    can also be split on cust_name.
 *    Again this split can cause a lot of extra work on the Oracle-side.
 *    Since blocks contain arbitrary column values, each query split task could end up
 *    doing the same work as a single query.
 *    - Certain situations are much better, performance wise:
 *      - If the grouping column is also a partition column
 *      - If th column is a low cardinality column with a index.
 *      - For star-joins if the grouping column is an alternate
 *        key(so it is functionally determined by the dimension key) then we can partition
 *        on the dimension key and drive the fact join via a join index.
 *        So for example if `customer_name` is a unique identifier for `customer` we
 *        can partition on customer_key ranges.
 *
 * The QuerySplitAnalyzer doesn't decide on the actual split strategy to use.
 * It only identifies candidate [[SplitCandidate]] (in the future we may extend this to be specific
 * columns in a candidate table). So for example for Join queries it only identifies that all
 * joining tables are Candidates. Picking a table for splitting is done by the [[OraSplitStrategy]]
 * once it has the [[PlanInfo plan statistics]] available.
 *
 * Currently we only generate split candidates for single table queries or inner join queries.
 */
object QuerySplitAnalyzer {

  private def splitDetails(splitTables : SplitCandidate*) =
    SplitScope(splitTables)


  def splitCandidates(oraPlan : OraPlan) : SplitScope = oraPlan match {
    case ot : OraTableScan => splitDetails(SplitCandidate(None, ot))
    case JoinPlan(sTbls) => sTbls
    case _ => SplitScope(Seq.empty)
  }

  private object JoinPlan {

    def isEquiJoinPlan(plan : LogicalPlan) : Boolean = plan match {
      case ExtractEquiJoinKeys(Inner, _, _, _, leftChild, rightChild, _) =>
        isEquiJoinPlan(leftChild) && isEquiJoinPlan(rightChild)
      case DataSourceV2ScanRelation(_, oraFScan: OraFileScan, _) =>
        !oraFScan.oraPlan.catalystOp.isDefined
      case DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
        isEquiJoinSubPlan(oraScan.oraPlan.catalystOp)
      case p : Project => isEquiJoinPlan(p.child)
      case f : Filter => isEquiJoinPlan(f.child)
      case g : GlobalLimit => isEquiJoinPlan(g.child)
      case l : LocalLimit => isEquiJoinPlan(l.child)
      case _ => false
    }

    /*
     * Must only contain EquiJoins, Table Scans, Projects or Filters
     */
    def isEquiJoinSubPlan(lp : Option[LogicalPlan]) : Boolean =
      lp.isDefined && isEquiJoinPlan(lp.get)

    def unapply(oPlan: OraSingleQueryBlock): Option[SplitScope] = {
      val noSubPlans = oPlan.children.forall(p => p.isInstanceOf[OraTableScan])

      if (noSubPlans && !oPlan.hasAggregate && isEquiJoinSubPlan(oPlan.catalystOp)) {
        val sTbls =
          SplitCandidate(oPlan.getSourceAlias, oPlan.source.asInstanceOf[OraTableScan]) +:
          oPlan.joins.
            map(j => SplitCandidate(j.getJoinAlias, j.joinSrc.asInstanceOf[OraTableScan])
            )
        Some(splitDetails(sTbls: _*))
      } else {
        None
      }
    }
  }
}
