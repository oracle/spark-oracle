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

// scalastyle:off println
class SetOpTranslationTest extends AbstractTranslationTest {

  testPushdown("union1",
    """
      |select c_int as val
      |from sparktest.unit_test
      |where c_int > 5
      |union all
      |select c_int + c_long
      |from sparktest.unit_test
      |where c_int <= 5
      |""".stripMargin,
    """select "C_INT" AS "val"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where ("C_INT" IS NOT NULL AND ("C_INT" > ?)) UNION ALL select ("C_INT" + "C_LONG") AS "1_sparkora"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where ("C_INT" IS NOT NULL AND ("C_INT" <= ?))""".stripMargin
  )

  testPushdown("intersect1",
    """
      |select c_int as val
      |from sparktest.unit_test
      |where c_int > 600000
      |intersect
      |select c_int
      |from sparktest.unit_test
      |where c_int < 10000000
      |""".stripMargin,
    """select "val"
      |from ( select "C_INT" AS "val"
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where (("C_INT" IS NOT NULL AND ("C_INT" > ?)) AND  (COALESCE("C_INT" , ?), CASE WHEN "C_INT" IS NULL THEN 1 ELSE 0 END) IN ( select COALESCE("C_INT" , 0), CASE WHEN "C_INT" IS NULL THEN 1 ELSE 0 END
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where ("C_INT" IS NOT NULL AND ("C_INT" < ?)) )) ) """.stripMargin + """
      |group by "val"""".stripMargin
      )

  // scalastyle:off line.size.limit
  testPushdown("except1",
    """
      |select c_int as val
      |from sparktest.unit_test
      |where c_int > 600000
      |except
      |select c_int
      |from sparktest.unit_test
      |where c_int > 10000000
      |""".stripMargin,
    """select "val"
      |from ( select "sparkora_0"."C_INT" AS "val"
      |from "SPARKTEST"."UNIT_TEST" "sparkora_0"
      |where (("sparkora_0"."C_INT" IS NOT NULL AND ("sparkora_0"."C_INT" > ?)) AND not exists ( select 1
      |from "SPARKTEST"."UNIT_TEST" """.stripMargin + """
      |where (("C_INT" IS NOT NULL AND ("C_INT" > ?)) AND ((COALESCE("sparkora_0"."C_INT" , ?) = COALESCE("C_INT" , ?)) AND (CASE WHEN "sparkora_0"."C_INT" IS NULL THEN 1 ELSE 0 END = CASE WHEN "C_INT" IS NULL THEN 1 ELSE 0 END))) )) ) """.stripMargin + """
      |group by "val"""".stripMargin
  )



}
