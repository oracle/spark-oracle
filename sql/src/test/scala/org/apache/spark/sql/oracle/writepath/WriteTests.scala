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

package org.apache.spark.sql.oracle.writepath

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hive.test.oracle.TestOracleHive


class WriteTests extends AbstractWriteTests {

  import AbstractWriteTests._

  ignore("printScenarioDetails") {td =>
    val scnInfos = scenarios.map(scenarioInfo)
    val (scnSummaries, scnPlans) = scnInfos.unzip

    // scalastyle:off println
    println(
      s"""Scenarios:
         |${scnSummaries.mkString("\n")}""".stripMargin)
    println(
      s"""Scenario Plans:
         |${scnPlans.mkString("\n")}""".stripMargin)

    // scalastyle:on println
  }

  for (scn <- scenarios) performTest(scn)
  for (scn <- compositeScenarios) performTest(scn)
  for (scn <- deleteScenarios) performTest(scn)

  test("negWrongQueryShapeTest") {td =>
    val ex = intercept[AnalysisException] {
      val df = getAroundTestBugDF(
        """
          |insert into unit_test_write
          |select C_CHAR_1, C_CHAR_5, C_VARCHAR2_10, C_VARCHAR2_40, C_NCHAR_1, C_NCHAR_5,
          |       C_NVARCHAR2_10, C_NVARCHAR2_40, C_BYTE, C_SHORT, C_INT, C_LONG, C_NUMBER,
          |       C_DECIMAL_SCALE_5, C_DECIMAL_SCALE_8, C_DATE
          |from spark_catalog.default.src_tab_for_writes""".stripMargin
      )
      df.show(1000, false)
    }

    assert(ex.getMessage.startsWith(
      "Cannot write to 'SPARKTEST.UNIT_TEST_WRITE', not enough data columns")
    )
  }

  test("negTruncateTest") {td =>
    val ex = intercept[AnalysisException] {
      TestOracleHive.sql("truncate table unit_test_write").
        show(1000, false)
    }

    assert(ex.getMessage.startsWith("TRUNCATE TABLE is not supported for v2 tables."))
  }

  // run to validate src_tab_for_writes data file and get stats on data
  ignore("validateSrcData") {t =>
    try {
      TestOracleHive.sql("use spark_catalog")
      TestOracleHive.sql("select * from default.src_tab_for_writes").show(1000, false)

      // count = 501
      TestOracleHive.sql(
        """select count(*)
          |from default.src_tab_for_writes
          |where c_byte < 0""".stripMargin)
        .show(1000, false)

      TestOracleHive.sql(
        """select state, channel, count(*)
          |from default.src_tab_for_writes
          |group by state, channel""".stripMargin)
        .show(1000, false)
    } finally {
      TestOracleHive.sql("use oracle.sparktest")
    }
  }


}
