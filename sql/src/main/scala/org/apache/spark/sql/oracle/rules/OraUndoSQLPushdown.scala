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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.oracle.OraPushdownScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation


/**
 * Functions to undo the pushdowns in [[OraSQLPushdownRule]]
 */
object OraUndoSQLPushdown {

  private def planTransformStream(plan : LogicalPlan)
                         (implicit transform : LogicalPlan => Option[LogicalPlan])
  : Stream[LogicalPlan] = {

    def stream(plan : LogicalPlan) : Stream[LogicalPlan] = {
      val nextPlan = transform(plan)
      if (!nextPlan.isDefined) {
        Stream.empty[LogicalPlan]
      } else {
        Stream.cons(nextPlan.get, stream(nextPlan.get))
      }

    }

    Stream.cons(plan, stream(plan))
  }

  private def undoNextOraPushdown(plan : LogicalPlan) : Option[LogicalPlan] = {
    val oraPushDSv2 = plan.collectFirst {
      case dsv2@DataSourceV2ScanRelation(_, oScan : OraPushdownScan, _)
        if oScan.oraPlan.catalystOp.isDefined => dsv2
    }

    oraPushDSv2.map { undoOp =>
      plan.transformUp {
        case op if op == undoOp =>
          undoOp.scan.asInstanceOf[OraPushdownScan].oraPlan.catalystOp.get
      }
    }
  }

  def refixNames(plan: LogicalPlan) : LogicalPlan =
    OraFixColumnNames(OraUnfixColumnNames.unfix(plan))

  def traceOraPushdown(plan : LogicalPlan)(callback : (LogicalPlan, Int) => Unit) : Unit = {
    val undoPlans = planTransformStream(plan)(pln => undoNextOraPushdown(pln).map(refixNames))
    var i : Int = 0
    for(p <- undoPlans) {
      callback(p, i)
      i += 1
    }
  }

  /**
   * Undo all the Pushdowns that will be executed as Shard Coordinator Queries.
   * @param plan
   * @return
   */
  def undoCoordinatorQueries(plan : LogicalPlan)
                            (replaceCoordPlan : LogicalPlan => Option[LogicalPlan]
                            ) : LogicalPlan = {
    val undoPlans = planTransformStream(plan)(replaceCoordPlan)
    val newPlan = undoPlans.last
    refixNames(newPlan)
  }

  /*
   * A more direct way of doing a trace
   * Here only for debug purpose.
   */
  private def trace(plan : LogicalPlan,
            stepNum : Int = 0)(
             fn : (LogicalPlan, Int) => Unit
           ) : LogicalPlan = {
    fn(plan, stepNum)

    val undoOp = plan.collectFirst {
      case dsv2@DataSourceV2ScanRelation(_, oScan : OraPushdownScan, _)
        if oScan.oraPlan.catalystOp.isDefined => dsv2
    }

    if (undoOp.isDefined) {
      val _nextPlan = plan.transformUp {
        case op if op == undoOp.get =>
          undoOp.get.scan.asInstanceOf[OraPushdownScan].oraPlan.catalystOp.get
      }
      val nextPlan = refixNames(_nextPlan)
      trace(nextPlan, stepNum + 1)(fn)
    } else {
      plan
    }
  }

}
