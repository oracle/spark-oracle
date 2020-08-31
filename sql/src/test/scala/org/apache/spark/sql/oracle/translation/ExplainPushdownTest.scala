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

import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.OraSparkConfig

class ExplainPushdownTest extends AbstractTranslationTest {

  test("rollup") { td =>
    TestOracleHive
      .sql("""
      |explain oracle pushdown select i_category
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
      |""".stripMargin)
      .show(10000, false)
  }

  test("rollup-pushdownoff") { td =>
    try {
      OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, false)

      TestOracleHive
        .sql("""
        |explain oracle pushdown select i_category
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
        |""".stripMargin)
        .show(10000, false)
    } finally {
      OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, true)
    }
  }

}
