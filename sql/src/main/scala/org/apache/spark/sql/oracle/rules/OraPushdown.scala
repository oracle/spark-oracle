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
import org.apache.spark.sql.catalyst.expressions.{AliasHelper, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.read.oracle.{OraPushdownScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.operators.OraQueryBlock

// scalastyle:off
/**
 * '''Table of Pushdown Constraints:'''
 * - Constraints on applying a relational operation in an existing [[OraQueryBlock]]
 * - yes means that the operation can be part of the same [[OraQueryBlock]]
 * - no means that a new [[OraQueryBlock]] is setup on which the operation is
 * applied. Existing Query Block becomes the source of this [[OraQueryBlock]]
 * {{{
 *QBlk has/  Column  Proj  Filt  Join  O-Join  S-Join  A-Join  L-Join  Agg
 *Apply      Pruned
 *--------------------------------------------------------------------------
 *Col. Prun  yes     yes   yes   yes   yes     yes     yes     yes     yes
 *Proj       yes     yes   yes   yes   yes     yes     yes     yes     yes
 *Filt       yes     yes   yes   yes   no      yes     no      yes     no
 *Join       yes     no    yes   yes   yes     no      no      no      no
 *Out-Join   yes     no    yes   yes   yes     yes     yes     yes     no
 *Semi-Join  yes     no    yes   yes   no      yes     yes     yes     no
 *Anti-Join
 *Lat-Join
 *Agg
 * }}}
 *
 * - Application of Project requires that AttrRefs in projections be substituted based
 * on current projections
 * - Application of Filter requires that AttrRefs in projections be substituted based
 * on current projections
 */
trait OraPushdown {
  // scalastyle:on
  val inDSScan: DataSourceV2ScanRelation
  val inOraScan: OraScan
  val inQBlk: OraQueryBlock
  val pushdownCatalystOp: LogicalPlan
  val sparkSession: SparkSession

  lazy val currQBlk = if (inQBlk.canApply(pushdownCatalystOp)) {
    inQBlk
  } else {
    OraQueryBlock.newBlockOnCurrent(inQBlk)
  }

  private[rules] def pushdownSQL: Option[OraQueryBlock]

  def pushdown: Option[DataSourceV2ScanRelation] = {
    pushdownSQL.map { oraPlan =>
      val newOraScan = OraPushdownScan(sparkSession, inOraScan.dsKey, oraPlan)
      inDSScan.copy(
        scan = newOraScan,
        output = pushdownCatalystOp.output.asInstanceOf[Seq[AttributeReference]]
      )
    }
  }

  /*
   * TODO: provide a partial pushdown translation mode.
   *  For Project, Filter, Order setup a plan where push as much
   * of projections, filters and sort order expressions.
   *
   * For example in UDTypesTest the query:
   * select oracle.unit_test_fn_2(col3) from sparktest.unit_test_table_udts
   * If the test does a `sql(query).show()` then the function invocation is not pushed
   * because the logical plan has a `cast(oracle.unit_test_fn_2_invoke(...), StringType)`
   * projection; and we don't pushdown a cast of a Struct to a String.
   * In this case should setup a plan like this:
   * Project cast(res_struct as String)
   *   DataSourceV2ScanRelation
   *     (OraPlan(orasql=select unit_test_fn_2(col3) from sparktest.unit_test_table_udts))
   * So partialPushdownSQL should return:
   *   (
   *     OraPlan(orasql=select unit_test_fn_2(col3) from sparktest.unit_test_table_udts)
   *     dsV2 => Project(cast(res_struct as String), dsV2)
   *   )
   *
   * Default behavior is to fallback to all or nothing mode.
   */
  private[rules] def partialPushdownSQL :
  (Option[OraQueryBlock], DataSourceV2ScanRelation => Option[LogicalPlan]) =
    (pushdownSQL, dsV2 => Some(dsV2))

  /*
   * Support partial pushdown of the pushdownCatalystOp.
   */
  def pushdownPartialMode: LogicalPlan = {
    partialPushdownSQL match {
      case (Some(oraPlan), fn) =>
        val newOraScan = OraPushdownScan(sparkSession, inOraScan.dsKey, oraPlan)
        val dsScanRel = inDSScan.copy(
          scan = newOraScan,
          output = pushdownCatalystOp.output.asInstanceOf[Seq[AttributeReference]]
        )
        fn(dsScanRel).getOrElse(pushdownCatalystOp)
      case (None, _) => pushdownCatalystOp
    }
  }
}

trait ProjectListPushdownHelper extends AliasHelper {

  /**
   * copied from [[CollapseProject]] rewrite rule.
   * - substitues refernces to aliases and `trimNonTopLevelAliases`
   * For example:
   * {{{
   *   Project(c + d as e,
   *           Project(a + b as c, d, DSV2...)
   *           )
   *  // becomes
   *  Project(a + b +d as e, DSV2...)
   * }}}
   * @param upper
   * @param lower
   * @return
   */
  def buildCleanedProjectList(upper: Seq[NamedExpression],
                              lower: Seq[NamedExpression]): Seq[NamedExpression] = {
    val aliases = getAliasMap(lower)
    upper.map(replaceAliasButKeepName(_, aliases))
  }
}