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

package org.apache.spark.sql.oracle.translation

// scalastyle:off line.size.limit println
class OrderTranslationTests extends AbstractTranslationTest {

  testPushdown("distinct",
    """select c_int as ci, c_long as cl,
      |       sum(distinct c_decimal_scale_8) + count(distinct c_decimal_scale_5)
      |from sparktest.unit_test
      |group by  c_int + c_long, c_int, c_long
      |having sum(distinct c_decimal_scale_8) + count(distinct c_decimal_scale_5) is null and c_int is null
      |order by ci, cl
      """.stripMargin
  )

  testPushdown("rollup1",
    """
      |select i_category
      |                  ,d_year
      |                  ,d_qoy
      |                  ,d_moy
      |                  ,s_store_id
      |                  ,sum(ss_sales_price*ss_quantity) sumsales
      |            from store_sales
      |                ,date_dim
      |                ,store
      |                ,item
      |       where  ss_sold_date_sk=d_date_sk
      |          and ss_item_sk=i_item_sk
      |          and ss_store_sk = s_store_sk
      |          and d_month_seq between 1200 and 1200+11
      |       group by  rollup(i_category, d_year, d_qoy, d_moy,s_store_id)
      |       order by i_category, d_year desc, d_qoy nulls first, d_moy desc nulls last
      |""".stripMargin
  )

  testPushdown("cube2",
    """
      |select i_category
      |                  ,d_year + d_qoy
      |                  ,s_store_id
      |                  ,sum(ss_sales_price*ss_quantity) sumsales
      |            from store_sales
      |                ,date_dim
      |                ,store
      |                ,item
      |       where  ss_sold_date_sk=d_date_sk
      |          and ss_item_sk=i_item_sk
      |          and ss_store_sk = s_store_sk
      |          and d_month_seq between 1200 and 1200+11
      |       group by  cube(i_category, d_year + d_qoy, s_store_id)
      |       order by i_category desc, 2 asc""".stripMargin
  )

  testPushdown("inSubquery",
    """select c_long
      |from sparktest.unit_test
      |where c_int in (select c_int
      |                from sparktest.unit_test_partitioned
      |                where c_long = sparktest.unit_test.c_long
      |                )
      |order by c_long
      |""".stripMargin,
    null,
    true,
    true)

  testPushdown(
    "notinSubquery",
    """
      |select c_long
      |from sparktest.unit_test
      |where c_int not in (select c_int
      |                from sparktest.unit_test_partitioned
      |                where c_long = sparktest.unit_test.c_long
      |                )
      |order by c_long desc nulls last""".stripMargin,
    null,
    true, true)

  testPushdown(
    "notexistsSubQuery",
    """
      |select c_long
      | from sparktest.unit_test
      | where not exists (select c_int
      |                 from sparktest.unit_test_partitioned
      |                 where c_long = sparktest.unit_test.c_long and
      |                       c_int = sparktest.unit_test.c_int
      |                 )
      |order by c_long desc nulls first""".stripMargin,
    null, true, true)

  testPushdown("union1",
    """
      |select c_int as val
      |from sparktest.unit_test
      |where c_int > 5
      |union all
      |select c_int + c_long
      |from sparktest.unit_test
      |where c_int <= 5
      |order by val
      |""".stripMargin)

  testPushdown("intersect1",
    """
      |select c_int as val
      |from sparktest.unit_test
      |where c_int > 600000
      |intersect
      |select c_int
      |from sparktest.unit_test
      |where c_int < 10000000
      |order by val
      |""".stripMargin
  )

}
