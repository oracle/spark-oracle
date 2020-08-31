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
import org.apache.spark.sql.types.IntegralType

trait Arithmetics { self : ExprTranslator =>

  import macroUniverse._

  object BasicArith {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      t match {
          /** Unfortunately cannot pattern match like this, see note on [[ExprBuilders]]
        case q"${l : sparkexpr.Expression} + ${r: sparkexpr.Expression }" =>
          Some(sparkexpr.Add(l, r))
           */
        case q"$lT + $rT" =>
          for ((l, r) <- binaryArgs(lT, rT))
            yield sparkexpr.Add(l, r)
        case q"$lT - $rT" =>
          for ((l, r) <- binaryArgs(lT, rT))
            yield sparkexpr.Subtract(l, r)
        case q"$lT * $rT" =>
          for ((l, r) <- binaryArgs(lT, rT))
            yield sparkexpr.Multiply(l, r)
        case q"$lT / $rT" =>
          for ((l, r) <- binaryArgs(lT, rT))
            yield
              if ( l.dataType.isInstanceOf[IntegralType] ) {
                sparkexpr.IntegralDivide(l, r)
              } else {
                sparkexpr.Divide(l, r)
              }
        case q"$lT % $rT" =>
          for ((l, r) <- binaryArgs(lT, rT))
            yield sparkexpr.Remainder(l, r)
        case _ => None
      }
  }

  object JavaMathFuncs {
    val mathCompanion = macroUniverse.typeOf[java.lang.Math].companion
    val absFuncs = mathCompanion.decl(TermName("abs"))

    def unapply(t: mTree): Option[sparkexpr.Expression] =
      t match {
        case q"$id(..$args)" if args.size == 1 && absFuncs.alternatives.contains(id.symbol) =>
          for (
            c <- CatalystExpression.unapply(args(0).asInstanceOf[mTree])
          ) yield sparkexpr.Abs(c)
        case _ => None
      }
  }

}
