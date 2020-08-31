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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.expressions.OraExpression
import org.apache.spark.sql.oracle.querysplit.OraSplitStrategy

case class OraCompositeQueryBlock(children : Seq[OraQueryBlock],
                                  catalystOp: Option[LogicalPlan],
                                  catalystProjectList: Seq[NamedExpression],
                                  oraCompOp : SQLSnippet
                                 ) extends OraQueryBlock {

  val firstChild : OraQueryBlock = children.head

  override def source: OraPlan =
    InternalFailure("attempt to access source of a composite query block", this)

  override def joins: Seq[OraJoinClause] =
    InternalFailure("attempt to access joins of a composite query block", this)

  override def latJoin: Option[OraLateralJoin] =
    InternalFailure("attempt to access latJoin of a composite query block", this)

  override def select: Seq[OraExpression] =
    InternalFailure("attempt to access select of a composite query block", this)

  override def where: Option[OraExpression] =
    InternalFailure("attempt to access where of a composite query block", this)

  override def groupBy: Option[Seq[OraExpression]] =
    InternalFailure("attempt to access groupBy of a composite query block", this)

  override def orderBy: Option[Seq[OraExpression]] =
    InternalFailure("attempt to access orderBy of a composite query block", this)

  override def canApply(plan: LogicalPlan): Boolean = false
  override def canApplyFilter : Boolean = false

  override def copyBlock(source: OraPlan,
                         joins: Seq[OraJoinClause],
                         latJoin: Option[OraLateralJoin],
                         select: Seq[OraExpression],
                         where: Option[OraExpression],
                         groupBy: Option[Seq[OraExpression]],
                         catalystOp: Option[LogicalPlan],
                         catalystProjectList: Seq[NamedExpression],
                         orderBy: Option[Seq[OraExpression]]): OraQueryBlock =
    InternalFailure("attempt to call copyBlock on a composite query block", this)

  override def orasql: SQLSnippet = SQLSnippet.join(children.map(_.orasql), oraCompOp)

  override def splitOraSQL(dbSplitId : Int, splitStrategy : OraSplitStrategy): SQLSnippet
  = SQLSnippet.join(children.map(c => c.splitOraSQL(dbSplitId, splitStrategy)), oraCompOp)
}
