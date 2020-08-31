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

package org.apache.spark.sql.oracle

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.UnsupportedAction
import org.apache.spark.sql.hive.test.oracle.TestOracleHive

class SupportsNamespacesTest extends AbstractTest {

  test("showNS") { td =>
    TestOracleHive.sql("show namespaces").show()
    TestOracleHive.sql("show current namespace").show()
  }

  test("descNS") { td =>
    intercept[NoSuchNamespaceException] {
      TestOracleHive.sql("describe namespace extended oracle").show()
    }

    TestOracleHive.sql("describe namespace extended oracle.tpcds").show(1000, false)

    TestOracleHive.sql("describe namespace extended oracle.sparktest").show()

  }

  test("nsDDL") { td =>
    var ex: Exception = intercept[UnsupportedAction] {
      TestOracleHive.sql("comment on namespace oracle.tpcds is 'this is a test'").show()
    }
    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: alter namespace: [SetProperty]
      | you should perform this using Oracle SQL""".stripMargin)

    ex = intercept[UnsupportedAction] {
      TestOracleHive.sql("create namespace oracle.newschema").show()
    }
    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: create namespace
          | create schema using Oracle DDL""".stripMargin)

    ex = intercept[UnsupportedAction] {
      TestOracleHive.sql("alter namespace oracle.tpcds set properties (prop1 = 'a')").show()
    }
    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: alter namespace: [SetProperty]
          | you should perform this using Oracle SQL""".stripMargin)

    ex = intercept[SparkException] {
      TestOracleHive.sql("drop namespace oracle.tpcds").show()
    }
    assert(
      ex.getMessage ==
        "Cannot drop a non-empty namespace: tpcds." +
          " Use CASCADE option to drop a non-empty namespace.")

    ex = intercept[UnsupportedAction] {
      TestOracleHive.sql("drop namespace oracle.sparktest cascade").show()
    }
    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: drop namespace
          | drop schema using Oracle DDL""".stripMargin)
  }

  test("showTables") { td =>
    TestOracleHive.sql("show current namespace").show()
    TestOracleHive.sql("show tables").show()

    TestOracleHive.sql("use TPCDS")
    TestOracleHive.sql("show current namespace").show()
    TestOracleHive.sql("show tables").show()

    TestOracleHive.sql("use tpcds")
    TestOracleHive.sql("show current namespace").show()
    TestOracleHive.sql("show tables").show(1000, false)

    TestOracleHive.sql("use SPARKTEST")
    TestOracleHive.sql("show current namespace").show()
    TestOracleHive.sql("show tables").show(1000, false)

  }
}
