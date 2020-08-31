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
package org.apache.spark.sql.oracle.rules

import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, EqualTo, Expression, IsNull, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.Join

/**
 * For not-in correlated subquery Spark generates a Join consition
 * which allows for the equality check of the left and right columns to be null.
 * So for the query:
 * {{{
 *  select c_long
 * from sparktest.unit_test
 * where c_int not in (select C_CURRENT_CDEMO_SK from customer
 *                 where c_long = C_CUSTOMER_SK
 *                 )
 * }}}
 * The [[LeftAntiJoin]] condition is:
 * {{{
 *   +- Join LeftAnti, (((cast(C_INT#11 as decimal(38,18)) = C_CURRENT_CDEMO_SK#20)
 *                           OR
 *                      isnull((cast(C_INT#11 as decimal(38,18)) = C_CURRENT_CDEMO_SK#20))
 *                    )
 *                     AND
 *                     (cast(cast(C_LONG#12L as decimal(20,0)) as decimal(38,18)) = C_CUSTOMER_SK#18))
 * }}}
 * The join condition for 'c_int' and 'c_currnt_cdemo_sk' has the 'isnull(..)' equality check
 * to allow for:
 * - `sparktest.unit_test` rows with null value to be output from the join
 * - all `sparktest.unit_test` rows to be output if there are any null rows in the right subquery.
 *
 * If the extra `join` condition matches this pattern then we translate the [[LeftAnti]]
 * joijn into a 'not in subquery' in Oracle SQL.
 */
case class NotInJoinPattern(join : Join) extends PredicateHelper {

  val left = join.left
  val right = join.right

  /**
   * - patterns copied from [[ExtractEquiJoinKeys]]
   * - not handling the case of [[EqualNullSafe]]
   */
  object Keys {

    def unapply(e : Expression) : Option[(Expression, Expression)] = e match {
      case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => Some((l, r))
      case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => Some((r, l))
      case _ => None
    }
  }

  def unapply(e : Expression) : Option[(Expression, Expression)] = e match {
    case Or(Keys(l1, r1), IsNull(Keys(l2, r2)))
      if (l1.canonicalized == l2.canonicalized &&
        r1.canonicalized == r2.canonicalized) => Some((l1, r1))
    case _ => None
  }
}
