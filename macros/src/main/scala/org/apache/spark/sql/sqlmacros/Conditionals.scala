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

import org.apache.spark.sql.catalyst.{expressions => sparkexpr}
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.types.BooleanType

/**
 * Translation for logical operators(AND, OR, NOT), for comparison operators
 * (`>, >=, <, <=, ==, !=`), string predicate functions(startsWith, endsWith, contains)
 * the `if` statement and the `case` statement.
 *
 * Support for case statements is limited:
 * - case pattern must be `cq"$pat => $expr2"`, so no `if` in case
 * - the pattern must be a literal for constructor pattern like `(a,b)`, `Point(1,2)` etc.
 *
 */
trait Conditionals { self: ExprTranslator =>

  import macroUniverse._

  val predUtilsTyp = typeOf[PredicateUtils.type ]
  val any2PredSym = predUtilsTyp.member(TermName("any2Preds"))

  val anyWithPredsTyp = typeOf[PredicateUtils.AnyWithPreds]

  val nullCheckMethods = Seq("is_null", "is_not_null")
  val null_safe_eq = "null_safe_eq"
  val inMethods = Seq("in", "not_in")

  private def isStrMethodCall(t : mTree) : Boolean =
    startsWithSyms.contains(t.symbol) || endsWithSym == t.symbol || containsSym == t.symbol

  private def boolExpr(expr : sparkexpr.Expression) : Option[sparkexpr.Expression] =
    expr.dataType match {
    case BooleanType => Some(expr)
    case dt if sparkexpr.Cast.canCast(dt, BooleanType) => Some(sparkexpr.Cast(expr, BooleanType))
    case _ => None
  }

  private def boolExprs(lTree : mTree,
                rTree : mTree) : Option[(sparkexpr.Expression, sparkexpr.Expression)] = {
    for (
      (lexpr, rexpr) <- binaryArgs(lTree, rTree);
      lboolExpr <- boolExpr(lexpr);
      rboolExpr <- boolExpr(rexpr)
    ) yield (lboolExpr, rboolExpr)
  }

  private def compareExprs(lTree : mTree,
                   rTree : mTree) : Option[(sparkexpr.Expression, sparkexpr.Expression)] = {
    for (
      (lexpr, rexpr) <- binaryArgs(lTree, rTree)
      if sparkexpr.RowOrdering.isOrderable(lexpr.dataType)
    ) yield (lexpr, rexpr)
  }

  object Predicates {

    private def nm(t : TermName) = t.decodedName.toString

    def unapply(t: mTree): Option[sparkexpr.Expression] =
      t match {
        case q"!$cond" =>
          for (
            expr <- CatalystExpression.unapply(cond);
            boolExpr <- boolExpr(expr)
          ) yield boolExpr
        case q"$lCond && $rCond" =>
          for (
            (l, r) <- boolExprs(lCond, rCond)
          ) yield sparkexpr.And(l, r)
        case q"$lCond || $rCond" =>
          for (
            (l, r) <- boolExprs(lCond, rCond)
          ) yield sparkexpr.Or(l, r)
        case q"$lT > $rT" =>
          for (
            (l, r) <- compareExprs(lT, rT)
          ) yield sparkexpr.GreaterThan(l, r)
        case q"$lT >= $rT" =>
          for (
            (l, r) <- compareExprs(lT, rT)
          ) yield sparkexpr.GreaterThanOrEqual(l, r)
        case q"$lT < $rT" =>
          for (
            (l, r) <- compareExprs(lT, rT)
          ) yield sparkexpr.LessThan(l, r)
        case q"$lT <= $rT" =>
          for (
            (l, r) <- compareExprs(lT, rT)
          ) yield sparkexpr.LessThanOrEqual(l, r)
        case q"$lT == $rT" =>
          for (
            (l, r) <- compareExprs(lT, rT)
          ) yield sparkexpr.EqualTo(l, r)
        case q"$lT != $rT" =>
          for (
            (l, r) <- compareExprs(lT, rT)
          ) yield sparkexpr.Not(sparkexpr.EqualTo(l, r))
        case q"$id(..$args).$m"
          if id.symbol == any2PredSym && args.size == 1 &&
            nullCheckMethods.contains(nm(m)) =>
          for (
            expr <- CatalystExpression.unapply(args(0))
          ) yield {
            if (nm(m) == "is_null") {
              sparkexpr.IsNull(expr)
            } else {
              sparkexpr.IsNotNull(expr)
            }
          }
        case q"$id(..$args1).$m(..$args2)"
          if id.symbol == any2PredSym && args1.size == 1 &&
            inMethods.contains(nm(m)) || nm(m) == null_safe_eq =>
          for (
            lexpr <- CatalystExpression.unapply(args1(0));
            inExprs <- CatalystExpressions.unapplySeq(args2)
          ) yield {
            if (nm(m) == "in") {
              sparkexpr.In(lexpr, inExprs)
            } else if (nm(m) == "not_in") {
              sparkexpr.Not(sparkexpr.In(lexpr, inExprs))
            } else {
              sparkexpr.EqualNullSafe(lexpr, inExprs.head)
            }
          }
        case q"$id(..$args)" if isStrMethodCall(id) =>
          id match {
            case q"$l.$m" =>
              for (
              lexpr <- CatalystExpression.unapply(l);
              rexpr <- CatalystExpression.unapply(args(0))
              ) yield {
                if ( nm(m) == "startsWith" ) {
                  sparkexpr.StartsWith(lexpr, rexpr)
                } else if ( nm(m) == "endsWith" ) {
                  sparkexpr.EndsWith(lexpr, rexpr)
                } else {
                  sparkexpr.Contains(lexpr, rexpr)
                }
              }
            case _ => None
          }
        case _ => None
      }
  }

  object IFCase {

    private def caseEntries(caseTrees : Seq[mTree]) : Option[Seq[(mTree, mTree)]] =
      OraSparkUtils.sequence(
      caseTrees map {
        case cq"$pat => $expr2" => Some((pat, expr2))
        case _ => None
      }
      )

    private def caseExpr(caseEntry : (mTree, mTree)) : Option[Seq[sparkexpr.Expression]]
    = caseEntry match {
      case (pq"_", t) => CatalystExpressions.unapplySeq(Seq(t))
      case (t1, t2) => CatalystExpressions.unapplySeq(Seq(t1, t2))
    }

    def unapply(t: mTree): Option[sparkexpr.Expression] = t match {
      case q"if ($cond) $thenp else $elsep" =>
        for (
          condExpr <- CatalystExpression.unapply(cond);
          thenExpr <- CatalystExpression.unapply(thenp);
          elseExpr <- CatalystExpression.unapply(elsep)
        ) yield sparkexpr.If(condExpr, thenExpr, elseExpr)
      case q"$expr match { case ..$cases } " =>
        for (
        matchExpr <- CatalystExpression.unapply((expr));
        caseEntries <- caseEntries(cases);
        exprs <- OraSparkUtils.sequence(caseEntries.map(caseExpr))
        ) yield sparkexpr.CaseKeyWhen(matchExpr, exprs.flatten)

        case _ => None
      }
  }
}
