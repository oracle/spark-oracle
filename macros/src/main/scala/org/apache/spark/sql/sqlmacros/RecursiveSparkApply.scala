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
package org.apache.spark.sql.sqlmacros

import org.apache.spark.sql.catalyst.{expressions => sparkexpr, FunctionIdentifier}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder

trait RecursiveSparkApply { self: ExprTranslator =>

  import macroUniverse._

  val reg_macros = typeOf[registered_macros.type]
  val dyn_applySym = reg_macros.member(TermName("applyDynamic"))

  private def funcBldr(nmLst : Seq[mTree]) : Option[FunctionBuilder] = {
    val macroNm = nmLst match {
      case Literal(Constant(x : String)) :: Nil => Some(x)
      case _ => None
    }
    for(
    nm <- macroNm;
    ss <- sparkSession;
    fb <- ss.sessionState.functionRegistry.lookupFunctionBuilder(FunctionIdentifier(nm))
    ) yield fb
  }

  object FunctionBuilderApplication {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      t match {
        case q"$id(..$nmLst)(..$args)" if args.size == 1 =>
          for (
            fb <- funcBldr(nmLst);
            exprs <- CatalystExpressions.unapplySeq(args)
          ) yield fb(exprs)
        case _ => None
      }
  }
}
