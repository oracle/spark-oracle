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

package org.apache.spark.sql.oracle.commands

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.{BaseSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.ExplainUtils.{getOpId, removeTags}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.oracle.querysplit.{OraSplitStrategy, PlanInfo}
import org.apache.spark.sql.oracle.sqlexec.SQLTemplate
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils


case class ExplainPushdown(sparkPlan: SparkPlan) extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())

  private def explainOraScan(dsv2 : BatchScanExec,
                             oraScan: OraScan,
                             append: String => Unit
                            )(
      implicit sparkSession: SparkSession
  ) : Unit = {
    val oPlan = oraScan.oraPlan
    val osql = oPlan.orasql

    val opName : String = {
      val opId = dsv2.getTagValue(QueryPlan.OP_ID_TAG).map(id => s"$id").getOrElse("unknown")
      s"($opId) ${dsv2.nodeName}"
    }

    append(s"${opName}\n")
    append(
      s"""Oracle Instance:
         |   ${oraScan.dsKey.toString}
         |""".stripMargin)

    append(
      s"""Pushdown Oracle SQL:
         |${SQLTemplate.buildPrintableSQL(osql.sql, osql.params)}
         |""".stripMargin

      )

    val (splitStrategy : OraSplitStrategy, planInfo : Option[PlanInfo]) = oraScan.explainPushdown

    if (planInfo.isDefined) {
      append("Pushdown Oracle SQL, oracle plan stats estimates:\n")
      planInfo.get.explain(append)
    }

    append("Pushdown Oracle SQL, Query Splitting details:\n")
    splitStrategy.explain(append)


  }

  private def processPlanSkippingSubqueries(plan : SparkPlan,
                                            append: String => Unit,
                                            startOpId : Int)(
    implicit sparkSession: SparkSession
  ) : Int = {
    var opId = startOpId

    def tagPlan(tp : QueryPlan[_]) : Unit = {
      tp foreach {
        case p : QueryPlan[_] =>
          if (p.getTagValue(QueryPlan.OP_ID_TAG).isEmpty) {
            opId += 1
            p.setTagValue(QueryPlan.OP_ID_TAG, opId)
          }
          p.innerChildren.foreach(p => tagPlan(p))
      }
    }

    try {
      tagPlan(plan)
      QueryPlan.append(
        plan,
        append,
        verbose = false,
        addSuffix = false,
        printOperatorId = true
      )

      append("\n")
      var i: Integer = 0

      plan foreach {
        case dsv2@BatchScanExec(_, oraScan: OraScan) =>
          explainOraScan(dsv2, oraScan, append)
        case _ => ()
      }

    } catch {
      case e: AnalysisException => append(e.toString)
    }
    opId
  }

  private def getSubqueries(plan : SparkPlan,
                            subqueries : ArrayBuffer[(SparkPlan, Expression, BaseSubqueryExec)]
                           ) : Unit = {
    plan.foreach {
      case p: SparkPlan =>
        p.expressions.foreach (_.collect {
          case e: PlanExpression[_] =>
            e.plan match {
              case s: BaseSubqueryExec =>
                subqueries += ((p, e, s))
                getSubqueries(s, subqueries)
              case _ =>
            }
        })
    }
  }

  private def processPlan(plan : SparkPlan,
                          append: String => Unit)(
    implicit sparkSession: SparkSession
  ) : Unit = {
    try {
      var currentOperatorID = 0
      currentOperatorID = processPlanSkippingSubqueries(plan, append, currentOperatorID)

      val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, BaseSubqueryExec)]
      getSubqueries(plan, subqueries)

      var i = 0

      for (sub <- subqueries) {
        if (i == 0) {
          append("\n===== Subqueries =====\n\n")
        }
        i = i + 1
        append(s"Subquery:$i Hosting operator id = " +
          s"${getOpId(sub._1)} Hosting Expression = ${sub._2}\n")

        currentOperatorID = processPlanSkippingSubqueries(
          sub._3,
          append,
          currentOperatorID)

        append("\n")
      }

    } finally {
      removeTags(plan)
    }
  }


  override def run(sparkSession: SparkSession): Seq[Row] = try {
    implicit val ss = sparkSession

    val concat = new PlanStringConcat()
    val  append: String => Unit = concat.append
    processPlan(sparkPlan, append)
    val outputString =
      Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, concat.toString)
    Seq(Row(outputString))
  } catch { case cause: TreeNodeException[_] =>
    (
    Seq("Error occurred during executing explain pushdown: ") ++
      cause.getMessage.split("\n")
      ).map(Row(_))
  }

}
