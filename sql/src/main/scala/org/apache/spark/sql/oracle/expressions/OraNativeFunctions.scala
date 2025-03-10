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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, V2Aggregator}
import org.apache.spark.sql.catalyst.expressions.{ApplyFunctionExpression, Expression}
import org.apache.spark.sql.connector.catalog.oracle.{OraNativeAggV1FuncInvoke, OraNativeAggV2FuncInvoke, OraNativeRowV1FuncInvoke, OraNativeRowV2FuncInvoke, OracleMetadata}

object OraNativeFunctions {

  /**
   * Handle any vagaries of oracle native function invocations here.
   * - only one so far is generate `USER` instead of `USER()`
   *
   * @param fnDef
   * @param cE
   * @param childOEs
   * @return
   */
  private def oraV1FnInvokeExpr(fnDef : OracleMetadata.OraFuncDef,
                        cE : Expression,
                        childOEs: Seq[OraExpression]) : OraExpression = {
    if (fnDef.owner == "SYS" && fnDef.name == "USER") {
      new OraLiteralSql("USER")
    } else {
      OraFnExpression(fnDef.orasql_fnname, cE, childOEs)
    }
  }

  private def oraV2FnInvokeExpr(fnName : String, fnOwner : String, orasql_fnname : String,
                      cE : Expression,
                      childOEs: Seq[OraExpression]) : OraExpression = {
    if (fnOwner == "SYS" && fnName== "USER") {
      new OraLiteralSql("USER")
    } else {
      OraFnExpression(orasql_fnname, cE, childOEs)
    }
  }


  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {

      case cE@ApplyFunctionExpression(OraNativeRowV2FuncInvoke(fnName, fnOwner, orasql_fnname, _, _),
      OraExpressions(oEs @ _*)) => oraV2FnInvokeExpr(fnName, fnOwner, orasql_fnname, cE, oEs)

      case cE@V2Aggregator(OraNativeAggV2FuncInvoke(fnName, fnOwner, orasql_fnname, _, _, _),
      OraExpressions(oEs @ _*), _, _) => oraV2FnInvokeExpr(fnName, fnOwner, orasql_fnname, cE, oEs)

      case cE@OraNativeRowV1FuncInvoke(fnDef, _, OraExpressions(oEs @ _*)) =>
        oraV1FnInvokeExpr(fnDef, cE, oEs)

      case cE@AggregateExpression(OraNativeAggV1FuncInvoke(fnDef, _, OraExpressions(oEs @ _*))
      , _, _, _, _) => oraV1FnInvokeExpr(fnDef, cE, oEs)

      case _ => null
    })

}
