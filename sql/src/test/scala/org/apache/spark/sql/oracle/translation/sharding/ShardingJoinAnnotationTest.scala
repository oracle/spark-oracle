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

package org.apache.spark.sql.oracle.translation.sharding

class ShardingJoinAnnotationTest extends AbstractShardingTranslationTest {

  test("basicJ") { td =>

    checkShardingInfo(
      """select l_returnflag
        |from lineitem join orders on l_orderkey = o_orderkey
        |where o_orderkey < 10
        |""".stripMargin,
      Set(0)
    )

  /*
   * In this case o_orderkey < 10 specified after outer join
   * - So any row with null o_orderkey will be eliminated
   * - So one can convert this into an inner join
   * - See rule: EliminateOuterJoin
   * - So inner join sharding rules apply
   */
    checkShardingInfo(
      """select l_returnflag
        |from lineitem left outer join orders on l_orderkey = o_orderkey
        |where o_orderkey < 10
        |""".stripMargin,
      Set(0)
    )

    // join with sub-query block aliases
    checkShardingInfo(
      """select l_returnflag
        |from lineitem join
        |     (select o_orderkey okey from orders where o_orderkey < 10) o on l_orderkey = okey
        |""".stripMargin,
      Set(0)
    )

    // left-outer with sub-query block condition
    checkShardingInfo(
      """select l_returnflag
        |from lineitem left outer join
        |     (select o_orderkey okey from orders where o_orderkey < 10) o on l_orderkey = okey
        |""".stripMargin,
      Set(0, 1, 2)
    )

    // replicated-shardtable join
    checkShardingInfo(
      """select c_custkey
        |from customer join orders on c_custkey = o_custkey
        |where o_orderkey < 10
        |""".stripMargin,
      Set(0)
    )

    // replicated-tables test
    checkReplicatedQuery(
      """select p_partkey
        |from part join partsupp on p_partkey = ps_partkey
        |where p_size = 15
        |""".stripMargin
    )

    // exists-subquery tests
    checkShardingInfo(
      """select okey
        |from (select o_orderkey okey from orders where o_orderkey < 10) o
        |where exists (
        |		select
        |			*
        |		from
        |			lineitem
        |		where
        |			l_orderkey = okey
        |			and l_commitdate < l_receiptdate
        |	)
        |""".stripMargin,
      Set(0)
    )

    checkShardingInfo(
      """select okey
        |from (select o_orderkey okey from orders) o
        |where exists (
        |		select
        |			*
        |		from
        |			lineitem
        |		where
        |			l_orderkey = okey
        |			and l_commitdate < l_receiptdate
        |	)
        |""".stripMargin,
      Set(0, 1, 2)
    )

    checkShardingInfo(
      """select okey
        |from (select o_orderkey okey from orders) o
        |where exists (
        |		select
        |			*
        |		from
        |			lineitem
        |		where
        |			l_orderkey = okey
        |     and l_orderkey < 10
        |			and l_commitdate < l_receiptdate
        |	)
        |""".stripMargin,
      Set(0)
    )

    // in-subquery tests
    checkShardingInfo(
      """select okey
        |from (select o_orderkey okey from orders where o_orderkey < 10) o
        |where okey in (
        |		select l_orderkey
        |		from lineitem
        |		where l_commitdate < l_receiptdate
        |	)
        |""".stripMargin,
      Set(0)
    )

    checkShardingInfo(
      """select okey
        |from (select o_orderkey okey from orders) o
        |where okey in (
        |		select l_orderkey
        |		from lineitem
        |		where l_commitdate < l_receiptdate
        |   and  l_orderkey < 10
        |	)
        |""".stripMargin,
      Set(0)
    )

    checkShardingInfo(
      """select okey
        |from (select o_orderkey okey from orders) o
        |where okey in (
        |		select l_orderkey
        |		from lineitem
        |		where l_commitdate < l_receiptdate
        |	)
        |""".stripMargin,
      Set(0, 1, 2)
    )

    // right-outer
    checkShardingInfo(
      """select l_returnflag
        |from lineitem right outer join
        |     (select o_orderkey okey from orders where o_orderkey < 10) o on l_orderkey = okey
        |""".stripMargin,
      Set(0)
    )

    // full-outer
    checkShardingInfo(
      """select l_returnflag
        |from lineitem full outer join
        |     (select o_orderkey okey from orders where o_orderkey < 10) o on l_orderkey = okey
        |""".stripMargin,
      Set(0, 1, 2)
    )

    // not exists
    checkShardingInfo(
      """select okey
        |from (select o_orderkey okey from orders) o
        |where not exists (
        |		select
        |			*
        |		from
        |			lineitem
        |		where
        |			l_orderkey = okey
        |     and l_orderkey < 10
        |			and l_commitdate < l_receiptdate
        |	)
        |""".stripMargin,
      Set(0)
    )

    // not in
    checkShardingInfo(
      """select okey
        |from (select o_orderkey okey from orders) o
        |where okey not in (
        |		select l_orderkey
        |		from lineitem
        |		where l_commitdate < l_receiptdate
        |   and  l_orderkey < 10
        |	)
        |""".stripMargin,
      Set(0)
    )
  }

}
