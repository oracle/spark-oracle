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

import org.apache.spark.sql.oracle.tpch.TPCHQueries

class ShardingAnnotationAggregateTest extends AbstractShardingTranslationTest {

  test("basicA") { td =>
    checkReplicatedQuery(TPCHQueries.q2)

    // tpch q3
    checkCoordinatorQuery(TPCHQueries.q3)
    // upto aggregate
    checkShardingInfo(
      """
                           |select
                           |	l_orderkey,
                           |	sum(l_extendedprice * (1 - l_discount)) as revenue,
                           |	o_orderdate,
                           |	o_shippriority
                           |from
                           |	customer,
                           |	orders,
                           |	lineitem
                           |where
                           |	c_mktsegment = 'BUILDING'
                           |	and c_custkey = o_custkey
                           |	and l_orderkey = o_orderkey
                           |	and o_orderdate < date '1995-03-15'
                           |	and l_shipdate > date '1995-03-15'
                           |group by
                           |	l_orderkey,
                           |	o_orderdate,
                           |	o_shippriority
                           |""".stripMargin,
      Set(0, 1, 2))

    // tpch q5
    checkCoordinatorQuery(TPCHQueries.q5)
    //  upto join
    checkShardingInfo(
      """
                        |select
                        |	n_name,
                        |	(l_extendedprice * (1 - l_discount)) as revenue
                        |from
                        |	customer,
                        |	orders,
                        |	lineitem,
                        |	supplier,
                        |	nation,
                        |	region
                        |where
                        |	c_custkey = o_custkey
                        |	and l_orderkey = o_orderkey
                        |	and l_suppkey = s_suppkey
                        |	and c_nationkey = s_nationkey
                        |	and s_nationkey = n_nationkey
                        |	and n_regionkey = r_regionkey
                        |	and r_name = 'ASIA'
                        |	and o_orderdate >= date '1994-01-01'
                        |	and o_orderdate < date '1994-01-01' + interval '1' year
                        |""".stripMargin,
      Set(0, 1, 2))

    // tpch q18
    checkCoordinatorQuery(TPCHQueries.q18)
    //  upto aggregate
    checkShardingInfo(
      """
          select
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            sum(l_quantity)
          from
            customer,
            orders,
            lineitem
          where
            o_orderkey in (
              select
                l_orderkey
              from
                lineitem
              group by
                l_orderkey having
                  sum(l_quantity) > 300
            )
            and c_custkey = o_custkey
            and o_orderkey = l_orderkey
          group by
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice
          """,
      Set(0, 1, 2))

    // q21
    // upto join without non-equality correlated predicates
    // - 'l2.l_suppkey <> l1.l_suppkey'
    // - 'and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate'
    checkShardingInfo(
      """
        |select
        |	s_name
        |from
        |	supplier,
        |	lineitem l1,
        |	orders,
        |	nation
        |where
        |	s_suppkey = l1.l_suppkey
        |	and o_orderkey = l1.l_orderkey
        |	and o_orderstatus = 'F'
        |	and l1.l_receiptdate > l1.l_commitdate
        |	and exists (
        |		select
        |			*
        |		from
        |			lineitem l2
        |		where
        |			l2.l_orderkey = l1.l_orderkey
        |
        |	)
        | and not exists (
        |		select
        |			*
        |		from
        |			lineitem l3
        |		where
        |			l3.l_orderkey = l1.l_orderkey
        |
        |	)
        |	and s_nationkey = n_nationkey
        |	and n_name = 'SAUDI ARABIA'
        |""".stripMargin,
      Set(0, 1, 2))

    // q20
    // the lineitem aggregation is a CoordQuery Plan.
    // so subsequent joins have to be CoordQuery
    checkCoordinatorQuery(TPCHQueries.q20)

    // q9
    // sub-query join
    checkShardingInfo(
      """select
                        |			n_name as nation,
                        |			year(o_orderdate) as o_year,
                        |			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                        |		from
                        |			part,
                        |			supplier,
                        |			lineitem,
                        |			partsupp,
                        |			orders,
                        |			nation
                        |		where
                        |			s_suppkey = l_suppkey
                        |			and ps_suppkey = l_suppkey
                        |			and ps_partkey = l_partkey
                        |			and p_partkey = l_partkey
                        |			and o_orderkey = l_orderkey
                        |			and s_nationkey = n_nationkey
                        |			and p_name like '%green%'""".stripMargin,
      Set(0, 1, 2))

    // q10
    // up to aggregate
    checkCoordinatorQuery(
      """select
                        |	c_custkey,
                        |	c_name,
                        |	sum(l_extendedprice * (1 - l_discount)) as revenue,
                        |	c_acctbal,
                        |	n_name,
                        |	c_address,
                        |	c_phone,
                        |	c_comment
                        |from
                        |	customer,
                        |	orders,
                        |	lineitem,
                        |	nation
                        |where
                        |	c_custkey = o_custkey
                        |	and l_orderkey = o_orderkey
                        |	and o_orderdate >= date '1993-10-01'
                        |	and o_orderdate < date '1993-10-01' + interval '3' month
                        |	and l_returnflag = 'R'
                        |	and c_nationkey = n_nationkey
                        |group by
                        |	c_custkey,
                        |	c_name,
                        |	c_acctbal,
                        |	c_phone,
                        |	n_name,
                        |	c_address,
                        |	c_comment""".stripMargin
    )

    // q13
    // agg sub-query is sharded query
    checkCoordinatorQuery(
      """select
                        |			c_custkey,
                        |			count(o_orderkey) as c_count
                        |		from
                        |			customer left outer join orders on
                        |				c_custkey = o_custkey
                        |				and o_comment not like '%special%requests%'
                        |		group by
                        |			c_custkey""".stripMargin)

    // q4
    // up to join is sharded query
    checkShardingInfo(
      """
                        |select
                        |	o_orderpriority
                        |from
                        |	orders
                        |where
                        |	o_orderdate >= date '1993-07-01'
                        |	and o_orderdate < date '1993-07-01' + interval '3' month
                        |	and exists (
                        |		select
                        |			*
                        |		from
                        |			lineitem
                        |		where
                        |			l_orderkey = o_orderkey
                        |			and l_commitdate < l_receiptdate
                        |	)
                        |""".stripMargin,
      Set(0, 1, 2))

  }
}
