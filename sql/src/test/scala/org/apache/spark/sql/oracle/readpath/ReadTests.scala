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

package org.apache.spark.sql.oracle.readpath

import org.apache.spark.sql.hive.test.oracle.TestOracleHive

class ReadTests  extends AbstractReadTests {

  test("scan_unit_test") {td =>
    TestOracleHive.sql(
      """
        |select * from unit_test
        |""".stripMargin
    ).show(1000, false)
  }

  test("scan_unit_test_partitioned") {td =>
    TestOracleHive.sql(
      """
        |select * from unit_test_partitioned
        |""".stripMargin
    ).show(1000, false)
  }

  test("filters") {td =>
    TestOracleHive.sql(
      """
        |select c_byte, c_short
        |from unit_test
        |where
        |    c_int > 1 and
        |    case
        |       when c_short > 0 then "positive"
        |       when c_short < 0 then "negative"
        |       else "zero"
        |     end in ("positive", "zero") and
        |    (c_int % 5) < (c_int * 5) and
        |    abs(c_long) > c_long and
        |    c_date is null and
        |    c_timestamp is not null
        |""".stripMargin
    ).show(1000, false)
  }

}
