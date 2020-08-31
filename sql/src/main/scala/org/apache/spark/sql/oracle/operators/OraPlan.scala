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

package org.apache.spark.sql.oracle.operators

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.oracle.{SQLSnippet, SQLSnippetProvider}
import org.apache.spark.sql.oracle.expressions.{OraExpression, OraExpressions}
import org.apache.spark.sql.oracle.querysplit.OraSplitStrategy
import org.apache.spark.sql.sources.Filter

/**
 * Represents an Oracle Query, captured as an Operator tree that can be
 * manipulated. In general an [[OraPlan]] is mapped to the catalyst
 * [[LogicalPlan]] it was mapped from, but for a Scan this is setup
 * from information in an
 * [[org.apache.spark.sql.connector.read.oracle.OraScanBuilder]] or a
 * [[org.apache.spark.sql.connector.read.oracle.OraScan]].
 * The reason for this is we have a chicken-and-egg problem: a
 * [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation]]
 * is built from a [[Scan]] and an
 * [[org.apache.spark.sql.connector.read.oracle.OraScan]] needs a [[LogicalPlan]].
 *
 */
abstract class OraPlan extends TreeNode[OraPlan] with SQLSnippetProvider {

  /*
   * Notes:
   * - catalystOp: in general OraPlan should be associated with one
   *   - but in case of a orascan Op there is no associated LogPlan
   *     this is built from an OraTable
   *     Here we have chicken-and-egg issue: DSv2Scan is built from to a OraScan
   *     which needs a reference to a LogPlan since it is a OraPlan
   *     So make catalystOp optional
   * - do we need to resolve AttrRef; how to convert AttrRef
   *   - can assume Attr in expressions are resolved
   *   - so conversion is to an OraAttr that references the underlying AttrRef
   */

  def catalystOp: Option[LogicalPlan]

  def catalystProjectList : Seq[NamedExpression]

  lazy val catalystAttributes : Seq[Attribute] = catalystProjectList.map(_.toAttribute)

  def orasql: SQLSnippet

  /**
   * Generate the SQL for the given dbSplitId and splitStrategy
   *
   * @param dbSplitId
   * @param splitStrategy
   * @return
   */
  def splitOraSQL(dbSplitId : Int, splitStrategy : OraSplitStrategy) : SQLSnippet

  override def simpleStringWithNodeId(): String = {
    val operatorId = catalystOp
      .flatMap(_.getTagValue(QueryPlan.OP_ID_TAG))
      .map(id => s"$id")
      .getOrElse("unknown")
    s"$nodeName ($operatorId)".trim
  }

  override def verboseString(maxFields: Int): String = simpleString(maxFields)

}

object OraPlan {

  def filter(
      oraPlan: OraTableScan,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): OraTableScan = {

    var plan = oraPlan

    if (dataFilters.nonEmpty) {
      for (datFil <- OraExpression.convert(dataFilters)) {
        plan = plan.filter(datFil, false)
      }
    }

    if (partitionFilters.nonEmpty) {
      val partFil = OraExpression.convert(partitionFilters)

      /*
       * Why must be throw exception here?
       * because [[PruneFileSourcePartitions]] doesn't apply pushed down partition
       * filters on top of new Scan.
       */
      if (!partFil.isDefined) {
        InternalFailure(
          "push partition filters",
          oraPlan,
          s"failed to create ora expression for partition filter:" +
            s" ${partitionFilters.mkString(",")}")
      }
      plan = plan.filter(partFil.get, true)
    }
    plan
  }

  def buildOraPlan(
      table: OraTable,
      requiredAttrs: Seq[Attribute],
      pushedFilters: Array[Filter]): OraTableScan = {

    // TODO and oraExpressions
    val pushedOraExpressions: Array[OraExpression] =
      pushedFilters.flatMap(OraExpression.convert(_, table).toSeq)

    val oraProjs = OraExpressions.unapplySeq(requiredAttrs)

    if (!oraProjs.isDefined) {
      InternalFailure(
        "build project list",
        null,
        s"failed create ora expressions for projectList: ${requiredAttrs.mkString(",")}")
    } else {

      OraTableScan(
        table,
        None,
        requiredAttrs,
        oraProjs.get,
        if (pushedOraExpressions.nonEmpty) Some(pushedOraExpressions.head) else None,
        None)
    }
  }

  /**
   * If Plan is such that most of the processing is on a single
   * table, then use its stats as an estimate for the Plan
   * @param oraPlan
   * @return
   */
  def useTableStatsForPlan(oraPlan: OraPlan): Option[OraTable] = {
    // TODO
    None
  }
}
