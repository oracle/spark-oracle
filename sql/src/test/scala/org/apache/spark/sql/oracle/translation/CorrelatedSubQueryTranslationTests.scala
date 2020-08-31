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

class CorrelatedSubQueryTranslationTests extends AbstractTranslationTest {

  testPushdown(
    "inSubquery",
    """select c_long
  |from sparktest.unit_test
  |where c_int in (select c_int
  |                from sparktest.unit_test_partitioned
  |                where c_long = sparktest.unit_test.c_long
  |                )
  |""".stripMargin,
  """select "C_LONG"
    |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
    |where  ("C_INT", "C_LONG") IN ( select "C_INT", "C_LONG"
    |from "SPARKTEST"."UNIT_TEST_PARTITIONED"  )""".stripMargin,
    true, true)

  testPushdown(
    "existsSubQuery",
    """
      | with ssales as
      | (select ss_item_sk
      | from store_sales
      | where ss_customer_sk = 8
      | ),
      | ssales_other as
      | (select ss_item_sk
      | from store_sales
      | where ss_customer_sk = 10
      | )
      | select ss_item_sk
      | from ssales
      | where exists (select ssales_other.ss_item_sk
      |               from ssales_other
      |               where ssales_other.ss_item_sk = ssales.ss_item_sk
      |               )""".stripMargin,
  """select "SS_ITEM_SK"
    |from "TPCDS"."STORE_SALES" """.stripMargin + """
    |where (("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) AND  "SS_ITEM_SK" IN ( select "SS_ITEM_SK"
    |from "TPCDS"."STORE_SALES" """.stripMargin + """
    |where ("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) ))""".stripMargin)

  testPushdown(
    "notinSubquery",
    """
      |select c_long
      |from sparktest.unit_test
      |where c_int not in (select c_int
      |                from sparktest.unit_test_partitioned
      |                where c_long = sparktest.unit_test.c_long
      |                )""".stripMargin,
  """select "sparkora_0"."C_LONG"
    |from "SPARKTEST"."UNIT_TEST" "sparkora_0"
    |where  "C_INT" NOT IN ( select "C_INT"
    |from "SPARKTEST"."UNIT_TEST_PARTITIONED" """.stripMargin + """
    |where ("sparkora_0"."C_LONG" = "C_LONG") )""".stripMargin,
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
      |                 )""".stripMargin,
  """select "sparkora_0"."C_LONG"
    |from "SPARKTEST"."UNIT_TEST" "sparkora_0"
    |where not exists ( select 1
    |from "SPARKTEST"."UNIT_TEST_PARTITIONED" """.stripMargin + """
    |where (("sparkora_0"."C_LONG" = "C_LONG") AND ("sparkora_0"."C_INT" = "C_INT")) )""".
    stripMargin,
    true, true)

  testPushdown(
    "notInLongNames",
    """
      |with sq as
      |(
      |select c_int as very_long_int_name_abcdefghijklmnopqrstuvwxyz12345,
      |       c_long as very_long__long_name_abcdefghijklmnopqrstuvwxyz12345
      |from sparktest.unit_test
      |)
      |select a.very_long__long_name_abcdefghijklmnopqrstuvwxyz12345
      |from sq a join sq b on
      |             a.very_long_int_name_abcdefghijklmnopqrstuvwxyz12345 = b.very_long__long_name_abcdefghijklmnopqrstuvwxyz12345
      |where (a.very_long_int_name_abcdefghijklmnopqrstuvwxyz12345 + b.very_long_int_name_abcdefghijklmnopqrstuvwxyz12345) not in (
      |                select c_int
      |                from sparktest.unit_test_partitioned
      |                where a.very_long__long_name_abcdefghijklmnopqrstuvwxyz12345 = sparktest.unit_test_partitioned.c_long
      |                )
      |""".stripMargin,
    """select "sparkora_0"."very_long__long_nam_2_sparkora"
      |from ( select "C_INT" AS "very_long_int_name__1_sparkora", "C_LONG" AS "very_long__long_nam_2_sparkora"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where "C_INT" IS NOT NULL ) "sparkora_0" join ( select "C_INT" AS "very_long_int_name__1_sparkora", "C_LONG" AS "very_long__long_nam_2_sparkora"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where "C_LONG" IS NOT NULL ) "sparkora_1" on ("sparkora_0"."very_long_int_name__1_sparkora" = "sparkora_1"."very_long__long_nam_2_sparkora")
      |where  ("sparkora_0"."very_long_int_name__1_sparkora" + "sparkora_1"."very_long_int_name__1_sparkora") NOT IN ( select "C_INT"
      |from "SPARKTEST"."UNIT_TEST_PARTITIONED" """.stripMargin + """
      |where ("sparkora_0"."very_long__long_nam_2_sparkora" = "C_LONG") )""".stripMargin,
    true, false
  )

}
