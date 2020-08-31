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

// scalastyle:off line.size.limit
class JoinTranslationTests extends AbstractTranslationTest {

  testPushdown("join_4way",
    """
      |select a.c_int, b.c_int, c.c_int, d.c_int
      |from sparktest.unit_test a,
      |     sparktest.unit_test_partitioned b,
      |     sparktest.unit_test c,
      |     sparktest.unit_test_partitioned d
      |where a.c_int = b.c_int and b.c_int = c.c_int and c.c_int = d.c_int""".stripMargin,
    """select "sparkora_0"."C_INT" AS "C_INT_1_sparkora", "sparkora_1"."C_INT" AS "C_INT_2_sparkora", "sparkora_2"."C_INT" AS "C_INT_3_sparkora", "sparkora_3"."C_INT" AS "C_INT_4_sparkora"
      |from "SPARKTEST"."UNIT_TEST" "sparkora_0" join "SPARKTEST"."UNIT_TEST_PARTITIONED" "sparkora_1" on ("sparkora_0"."C_INT" = "sparkora_1"."C_INT") join "SPARKTEST"."UNIT_TEST" "sparkora_2" on ("sparkora_1"."C_INT" = "sparkora_2"."C_INT") join "SPARKTEST"."UNIT_TEST_PARTITIONED" "sparkora_3" on ("sparkora_2"."C_INT" = "sparkora_3"."C_INT")
      |where ((("sparkora_0"."C_INT" IS NOT NULL AND "sparkora_1"."C_INT" IS NOT NULL) AND "sparkora_2"."C_INT" IS NOT NULL) AND "sparkora_3"."C_INT" IS NOT NULL)""".stripMargin,
    true, true
  )

  testPushdown("join_4way_with_aliases",
    """
      |select a.c_int, b.c_int, c.c_int, d.c_int
      |from (select c_int + 1 as c_int from sparktest.unit_test) a,
      |     (select c_int + 1 as c_int from sparktest.unit_test_partitioned) b,
      |     (select c_int + 1 as c_int from sparktest.unit_test) c,
      |     (select c_int + 1 as c_int from sparktest.unit_test_partitioned) d
      |where a.c_int = b.c_int and b.c_int = c.c_int and c.c_int = d.c_int""".stripMargin,
    """select "sparkora_0"."c_int" AS "c_int_1_sparkora", "sparkora_1"."c_int" AS "c_int_2_sparkora", "sparkora_2"."c_int" AS "c_int_3_sparkora", "sparkora_3"."c_int" AS "c_int_4_sparkora"
      |from ( select ("C_INT" + 1) AS "c_int"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where ("C_INT" + ?) IS NOT NULL ) "sparkora_0" join ( select ("C_INT" + 1) AS "c_int"
      |from "SPARKTEST"."UNIT_TEST_PARTITIONED" """.stripMargin + """
      |where ("C_INT" + ?) IS NOT NULL ) "sparkora_1" on ("sparkora_0"."c_int" = "sparkora_1"."c_int") join ( select ("C_INT" + 1) AS "c_int"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where ("C_INT" + ?) IS NOT NULL ) "sparkora_2" on ("sparkora_1"."c_int" = "sparkora_2"."c_int") join ( select ("C_INT" + 1) AS "c_int"
      |from "SPARKTEST"."UNIT_TEST_PARTITIONED" """.stripMargin + """
      |where ("C_INT" + ?) IS NOT NULL ) "sparkora_3" on ("sparkora_2"."c_int" = "sparkora_3"."c_int")""".stripMargin,
    true, true
  )

  testPushdown("l_outer_q5",
    """
      |select ws_web_site_sk as wsr_web_site_sk,
      |           wr_returned_date_sk as date_sk,
      |           ws_sales_price as sales_price,
      |           ws_net_profit as profit,
      |           wr_return_amt as return_amt,
      |           wr_net_loss as net_loss
      |    from web_returns left outer join web_sales on
      |         ( wr_item_sk = ws_item_sk
      |           and wr_order_number = ws_order_number)
      |           """.stripMargin,
    """select "WS_WEB_SITE_SK" AS "wsr_web_site_sk", "WR_RETURNED_DATE_SK" AS "date_sk", "WS_SALES_PRICE" AS "sales_price", "WS_NET_PROFIT" AS "profit", "WR_RETURN_AMT" AS "return_amt", "WR_NET_LOSS" AS "net_loss"
      |from "TPCDS"."WEB_RETURNS"  left outer join ( select "WS_ITEM_SK", "WS_WEB_SITE_SK", "WS_ORDER_NUMBER", "WS_SALES_PRICE", "WS_NET_PROFIT"
      |from "TPCDS"."WEB_SALES"  )  on (("WR_ITEM_SK" = "WS_ITEM_SK") AND ("WR_ORDER_NUMBER" = "WS_ORDER_NUMBER"))""".stripMargin
  )

  testPushdown("full_outer_51",
  """select case when web.ws_item_sk is not null then web.ws_item_sk else store.ss_item_sk end item_sk
    |                 ,case when web.ws_sold_date_sk is not null then web.ws_sold_date_sk else store.ss_sold_date_sk end d_date
    |                 ,web.ws_sales_price web_sales
    |                 ,store.ss_sales_price store_sales
    |           from web_sales web full outer join store_sales store on (web.ws_item_sk = store.ss_item_sk
    |                                                          and web.ws_sold_date_sk = store.ss_sold_date_sk)""".stripMargin,
  """select CASE WHEN "WS_ITEM_SK" IS NOT NULL THEN "WS_ITEM_SK" ELSE "SS_ITEM_SK" END AS "item_sk", CASE WHEN "WS_SOLD_DATE_SK" IS NOT NULL THEN "WS_SOLD_DATE_SK" ELSE "SS_SOLD_DATE_SK" END AS "d_date", "WS_SALES_PRICE" AS "web_sales", "SS_SALES_PRICE" AS "store_sales"
    |from "TPCDS"."WEB_SALES"  full outer join ( select "SS_SOLD_DATE_SK", "SS_ITEM_SK", "SS_SALES_PRICE"
    |from "TPCDS"."STORE_SALES"  )  on (("WS_ITEM_SK" = "SS_ITEM_SK") AND ("WS_SOLD_DATE_SK" = "SS_SOLD_DATE_SK"))""".stripMargin)

  testPushdown("x-join",
    """
      |select a.c_int
      |from sparktest.unit_test a cross join sparktest.unit_test b
      |     join sparktest.unit_test c on a.c_int = c.c_int
      |where a.c_int between 100000 and 7000000 and
      |      b.c_int between 100000 and 7000000 and
      |      c.c_int between 100000 and 7000000""".stripMargin
  )

}
