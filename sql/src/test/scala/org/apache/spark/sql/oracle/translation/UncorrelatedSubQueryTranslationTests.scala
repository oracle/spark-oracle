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
class UncorrelatedSubQueryTranslationTests extends AbstractTranslationTest {
  testPushdown("inSubQuery",
    """
      |select c_long
      |from sparktest.unit_test
      |where c_int in (select c_int
      |                from sparktest.unit_test_partitioned
      |                where c_int > 5
      |                )
      |""".stripMargin,
    // why the funny form: """.stripMargin + """
    //   have to compensate for idea behavior of removing trailing spaces
    //   if you remove the """.stripMargin + """ the trailing space
    //   disappears after a compile.
    """select "C_LONG"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where  "C_INT" IN ( select "C_INT"
      |from "SPARKTEST"."UNIT_TEST_PARTITIONED" """.stripMargin + """
      |where ("C_INT" IS NOT NULL AND ("C_INT" > ?)) )""".stripMargin,
    true, true
  )

  testPushdown("notinSubQuery",
    """
      |select c_long
      |from sparktest.unit_test
      |where c_int not in (select c_int
      |                from sparktest.unit_test_partitioned
      |                where c_int > 5
      |                )
      |""".stripMargin,
    """select "C_LONG"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where  "C_INT" NOT IN ( select "C_INT"
      |from "SPARKTEST"."UNIT_TEST_PARTITIONED" """.stripMargin + """
      |where ("C_INT" IS NOT NULL AND ("C_INT" > ?)) )""".stripMargin,
    true, true
  )

  testPushdown("existsSubQuery",
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
      |
      |               )
      |""".stripMargin,
    """select "SS_ITEM_SK"
      |from "TPCDS"."STORE_SALES" """.stripMargin + """
      |where (("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) AND exists  ( select 1 AS "col"
      |from "TPCDS"."STORE_SALES" """.stripMargin + """
      |where (("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) AND rownum <= 1) ))""".
      stripMargin
  )

  testPushdown("notExistsSubQuery",
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
      | where not exists (select ssales_other.ss_item_sk
      |               from ssales_other
      |               )
      |""".stripMargin,
    """select "SS_ITEM_SK"
      |from "TPCDS"."STORE_SALES" """.stripMargin + """
      |where (("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) AND not exists  ( select 1 AS "col"
      |from "TPCDS"."STORE_SALES" """.stripMargin + """
      |where (("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) AND rownum <= 1) ))""".stripMargin
  )
}
