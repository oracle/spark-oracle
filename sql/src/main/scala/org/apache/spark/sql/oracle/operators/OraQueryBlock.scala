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

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.oracle.{OraSQLImplicits, SQLSnippet, SQLSnippetProvider}
import org.apache.spark.sql.oracle.expressions.{OraExpression, OraExpressions}

case class OraJoinClause(joinType: JoinType, joinSrc: OraPlan, onCondition: Option[OraExpression]) {
  import OraSQLImplicits._

  lazy val joinTypeSQL: SQLSnippet = joinType match {
    case Cross => osql"cross join"
    case Inner if !onCondition.isDefined => osql"cross join"
    case Inner => osql"join"
    case LeftOuter => osql"left outer join"
    case RightOuter => osql"right outer join"
    case FullOuter => osql"full outer join"
    case _ => null
  }

  def setJoinAlias(a: String): Unit = {
    joinSrc.setTagValue(OraQueryBlock.ORA_JOIN_ALIAS_TAG, a)
  }
  def getJoinAlias: Option[String] =
    joinSrc.getTagValue(OraQueryBlock.ORA_JOIN_ALIAS_TAG)
  def clearJoinAlias : Unit = joinSrc.unsetTagValue(OraQueryBlock.ORA_JOIN_ALIAS_TAG)

  def joinSrcSQL(implicit srcSnipOverride : OraTableScan => Option[SQLSnippet] = t => None)
  : SQLSnippet = {
    val srcSQL = joinSrc match {
      case ot: OraTableScan => srcSnipOverride(ot).getOrElse(SQLSnippet.tableQualId(ot.oraTable))
      case oQ: OraQueryBlock => SQLSnippet.subQuery(oQ.orasql)
    }

    val qualifier: SQLSnippet =
      getJoinAlias.map(jA => SQLSnippet.colRef(jA)).getOrElse(SQLSnippet.empty)

    srcSQL + qualifier

  }

  def orasql(implicit srcSnipOverride : OraTableScan => Option[SQLSnippet] = t => None) : SQLSnippet
  = if (onCondition.isDefined) {
    osql"${joinTypeSQL + joinSrcSQL} on ${onCondition.get.reifyLiterals}"
  } else {
    osql"${joinTypeSQL + joinSrcSQL}"
  }
}

trait OraQueryBlock extends OraPlan with Product {

  import OraQueryBlock._

  def source: OraPlan
  def joins: Seq[OraJoinClause]
  def latJoin: Option[OraLateralJoin]
  def select: Seq[OraExpression]
  def where: Option[OraExpression]
  def groupBy: Option[Seq[OraExpression]]
  def catalystOp: Option[LogicalPlan]
  def catalystProjectList: Seq[NamedExpression]
  def orderBy : Option[Seq[OraExpression]]

  def canApply(plan: LogicalPlan): Boolean
  def canApplyFilter : Boolean

  def getSourceAlias: Option[String] = getTagValue(ORA_SOURCE_ALIAS_TAG)
  def setSourceAlias(alias: String): Unit = {
    setTagValue(ORA_SOURCE_ALIAS_TAG, alias)
  }

  def copyBlock(
      source: OraPlan = source,
      joins: Seq[OraJoinClause] = joins,
      latJoin: Option[OraLateralJoin] = latJoin,
      select: Seq[OraExpression] = select,
      where: Option[OraExpression] = where,
      groupBy: Option[Seq[OraExpression]] = groupBy,
      catalystOp: Option[LogicalPlan] = catalystOp,
      catalystProjectList: Seq[NamedExpression] = catalystProjectList,
      orderBy: Option[Seq[OraExpression]] = orderBy): OraQueryBlock

}

object OraQueryBlock {
  val ORA_SOURCE_ALIAS_TAG = TreeNodeTag[String]("_oraSourceAlias")

  /**
   * Start a new OraQueryBlock on top of the current block.
   * @return
   */
  def newBlockOnCurrent(currQBlock : OraQueryBlock) : OraQueryBlock = {
    val newOraExprs = OraExpressions.unapplySeq(currQBlock.catalystAttributes).get
    OraSingleQueryBlock(currQBlock, Seq.empty, None, newOraExprs,
      None, None, None, currQBlock.catalystAttributes, None)
  }

  val ORA_JOIN_ALIAS_TAG = TreeNodeTag[String]("_joinAlias")
}
