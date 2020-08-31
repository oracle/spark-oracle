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
package org.apache.spark.sql.oracle.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, ListQuery, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.operators.{OraPlan, OraQueryBlock}
import org.apache.spark.sql.oracle.rules.OraSQLPushdownRule

/**
 * Conversions for expressions in subquery.scala
 */
object Subquery {
  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case sq@ScalarSubquery(plan, _, _) =>
        val pushdownPlan = OraSQLPushdownRule(plan)
        pushdownPlan match {
          case DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
            OraSubQuery(sq, oraScan.oraPlan)
          case _ => null
        }
      case lq@ListQuery(plan, _, _, _) =>
        val pushdownPlan = OraSQLPushdownRule(plan)
        pushdownPlan match {
          case DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
            OraSubQuery(lq, oraScan.oraPlan)
          case _ => null
        }
      case _ => null
    })

  /**
   * Marker trait for ''sub-query'' expressions.
   */
  trait OraSubqueryExpression extends OraExpression {
    def oraPlan : OraPlan
  }

  /**
   * Represents a subQuery check (such as IN or NOT IN or NOT EXISTS)
   * This is the Oracle SQL for a Spark [[LeftSemi]] or [[LeftAnti]]
   * `join` operattion.
   *
   * @param joinOp
   * @param joiningExprs
   * @param op
   * @param oraPlan
   */
  case class OraSubQueryJoin(joinOp : Join,
                             joiningExprs : Seq[OraExpression],
                             op : SQLSnippet,
                             oraPlan : OraQueryBlock) extends OraSubqueryExpression {
    override def catalystExpr: Expression = joinOp.condition.get

    override def orasql: SQLSnippet = {
      val joinExprsSQL : Seq[SQLSnippet] = joiningExprs.map(_.orasql)
      val subQrySQL = oraPlan.orasql

      if (joiningExprs.size == 0) {
        osql"${op} ( ${subQrySQL} )"
      } else if (joinExprsSQL.size > 1) {
        osql" (${SQLSnippet.csv(joinExprsSQL : _*)}) ${op} ( ${subQrySQL} )"
      } else {
        osql" ${joinExprsSQL.head} ${op} ( ${subQrySQL} )"
      }
    }

    override val children: Seq[OraExpression] = joiningExprs
  }

  case class OraSubQuery(catalystExpr : SubqueryExpression,
                         oraPlan : OraPlan) extends OraSubqueryExpression {
    override def orasql: SQLSnippet =
      osql" ( ${oraPlan} )"
    override val children: Seq[OraExpression] = Seq.empty
  }

  /**
   * Captures snippet that represents:  `exists` | `not exists` sub-query
   * @param catalystExpr
   * @param sq
   */
  case class OraNullCheckSubQuery(catalystExpr : Expression,
                                  op : SQLSnippet,
                                  sq : OraSubQuery) extends OraExpression {
    override def orasql: SQLSnippet = osql"${op} ${sq.orasql}"
    override val children: Seq[OraExpression] = Seq(sq)
  }
}
