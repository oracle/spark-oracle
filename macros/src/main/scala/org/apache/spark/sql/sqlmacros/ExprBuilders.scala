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

import org.apache.spark.sql.catalyst.{expressions => sparkexpr, InternalRow}
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.types.StringType

/**
 * Writing match Patterns:
 * - One cannot write a pattern like this
 *   {{{q"${l : sparkexpr.Expression} + ${r: sparkexpr.Expression }"}}}
 *   because the tree for {{{1 + 2}}} is
 *   {{{Apply(Select(Literal(Constant(1)), TermName("$plus")), List(Literal(Constant(2))))}}}
 *   and the tree for {{{Array(5}}} is
 *   {{{Apply(Select(Ident(scala.Array), TermName("apply")), List(Literal(Constant(5))))}}}
 *   Pattern matching for {{{Array(5}}} triggers a recursive unapply on
 *   {{{CatalystExpression}}} for the subtree {{{Ident(scala.Array)}}}, which doesn't match
 *   any valid ''Expression'' pattern; so we attempt to ''evaluate'' the tree, and
 *   this fails.
 */
trait ExprBuilders
  extends Arithmetics with Collections with Structs with Tuples
with DateTime with Options with RecursiveSparkApply with Conditionals
with Strings { self : ExprTranslator =>

  import macroUniverse._

  val predefTyp = typeOf[scala.Predef.type]

  object Literals {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      Option(t match {
        case Literal(Constant(v)) =>
          scala.util.Try(sparkexpr.Literal(v)).toOption.orNull
        case _ => null
      })
  }

  object Reference {

    private def exprInScope(nm : TermName) : Option[sparkexpr.Expression] =
      scope.get(nm).map(_.rhsExpr)

    def unapply(t: mTree): Option[sparkexpr.Expression] =
      Option(t match {
        case Ident(tNm : TermName) => exprInScope(tNm).orNull
        case tNm : TermName => exprInScope(tNm).orNull
        case _ => null
      })
  }

  object StaticValue {
    def unapply(t: mTree): Option[sparkexpr.Expression] = {
      if (t.tpe =:= typeOf[org.apache.spark.unsafe.types.UTF8String]) {
        doWithWarning[sparkexpr.Expression](t,
          "evaluate to a static value",
          {
            val v = eval_tree(t)
            new sparkexpr.Literal(v, StringType)
          })
      } else {
        (for (typInfo <- TypeInfo.unapply(t)) yield {
          doWithWarning[sparkexpr.Expression](t,
            "evaluate to a static value",
            {
              val v = eval_tree(t)
              val iRow = InternalRow(v)
              val lVal = typInfo.exprEnc.objSerializer.eval(iRow)
              new sparkexpr.Literal(lVal, typInfo.catalystType)
            })
        }).flatten
      }
    }
  }

  private[sqlmacros] def binaryArgs(lT : mTree, rT : mTree) :
  Option[(sparkexpr.Expression, sparkexpr.Expression)] = {
    for (
      l <- CatalystExpression.unapply(lT);
      r <- CatalystExpression.unapply(rT)
    ) yield (l, r)
  }

  object CatalystExpression {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      Option(t match {
        case Literals(e) => e
        case Reference(e) => e
        case BasicArith(e) => e
        case StringPatterns(e) => e
        case JavaMathFuncs(e) => e
        case CollectionTrees(e) => e
        case TupleConstruct(e) => e
        case FieldAccess(e) => e
        case StructConstruct(e) => e
        case DateTimePatterns(e) => e
        case OptionPatterns(e) => e
        case Predicates(e) => e
        case IFCase(e) => e
        case FunctionBuilderApplication(e) => e
        case StaticValue(e) => e
        case _ => null
      })
  }

  object CatalystExpressions {
    def unapplySeq(tS: Seq[mTree]): Option[Seq[sparkexpr.Expression]] =
      OraSparkUtils.sequence(tS.map(CatalystExpression.unapply(_)))
  }

  implicit val toExpr = new Unliftable[sparkexpr.Expression] {
    def unapply(t: c.Tree): Option[sparkexpr.Expression] = CatalystExpression.unapply(t)
  }

}
