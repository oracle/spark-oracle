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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.oracle.{expressions, SQLSnippet}
import org.apache.spark.sql.oracle.expressions.{OraBinaryOpExpression, OraExpression}
import org.apache.spark.sql.oracle.querysplit.OraSplitStrategy

case class OraTableScan(
                         oraTable: OraTable,
                         catalystOp: Option[LogicalPlan],
                         catalystProjectList : Seq[Attribute],
                         projections: Seq[OraExpression],
                         filter: Option[OraExpression],
                         partitionFilter: Option[OraExpression])
  extends OraPlan { self =>

  override lazy val catalystAttributes : Seq[Attribute] = catalystProjectList

  val children: Seq[OraPlan] = Seq.empty
  override def stringArgs: Iterator[Any] =
    Iterator(SQLSnippet.tableQualId(oraTable).sql, catalystProjectList, projections, filter)

  def filter(oFil: OraExpression, isPartFilter: Boolean): OraTableScan = {

    def setFil(os: OraTableScan) = {
      val currFilt = if (!isPartFilter) {
        os.filter
      } else {
        os.partitionFilter
      }

      val newFil = if (currFilt.isDefined) {
        val fil = currFilt.get
        OraBinaryOpExpression(expressions.AND,
          And(fil.catalystExpr, oFil.catalystExpr),
          fil, oFil)
      } else oFil

      if (!isPartFilter) {
        os.copy(filter = Some(newFil))
      } else {
        os.copy(partitionFilter = Some(newFil))
      }
    }

    self match {
      case os: OraTableScan => setFil(os)
      case _ =>
        IllegalAction(
          "filter",
          self,
          "on a OraPlan that is not a scan, provide associated catalyst filter op")
    }
  }

  private def filSnippet : Option[SQLSnippet] =
    SQLSnippet.combine(SQLSnippet.AND, filter.map(_.orasql), partitionFilter.map(_.orasql))

  override def orasql: SQLSnippet = {
    SQLSnippet.select(projections.map(_.orasql): _*).from(oraTable).where(filSnippet)
  }

  /*
   * Only called when pushdown is off.
   * Provide same sql gen as sourcaTable in [[OraSingleQueryBlock]]
   */
  override def splitOraSQL(dbSplitId : Int, splitStrategy : OraSplitStrategy): SQLSnippet = {
    val tblCl =
      splitStrategy.splitOraSQL(this, dbSplitId).getOrElse(SQLSnippet.tableQualId(oraTable))
    SQLSnippet.select(projections.map(_.orasql): _*).
      from(tblCl).where(filSnippet)
  }

  def isQuerySplitCandidate: Boolean =
    getTagValue(OraTableScan.ORA_QUERY_SPLIT_CANDIDATE_TAG).getOrElse(false)
  def setQuerySplitCandidate : Unit = {
    setTagValue(OraTableScan.ORA_QUERY_SPLIT_CANDIDATE_TAG, true)
  }
}

object OraTableScan {
  val ORA_QUERY_SPLIT_CANDIDATE_TAG = TreeNodeTag[Boolean]("_oraQuerySplitCandidate")
}

