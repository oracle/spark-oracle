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

import oracle.spark.ConnectionManagement
import org.scalactic.source
import org.scalatest.Tag

import org.apache.spark.sql.connector.catalog.oracle.OracleCatalog
import org.apache.spark.sql.hive.test.oracle.TestOracleHive

abstract class ShardingAbstractTest extends AbstractTest {

  lazy val dsKey = OracleCatalog.oracleCatalog.getMetadataManager.dsKey
  lazy val isTestInstanceSharded = ConnectionManagement.info(dsKey).isSharded

  class ShardResultOfTestInvocation(testName: String, testTags: Tag*)
    extends ResultOfTestInvocation(testName, testTags : _*) {
    override def apply(testFun: FixtureParam => Any /* Assertion */)
                      (implicit pos: source.Position): Unit = {
      super.apply { fixParam =>
        if (isTestInstanceSharded) {
          testFun(fixParam)
        }
      }
    }

    override def apply(testFun: () => Any /* Assertion */)
                      (implicit pos: source.Position): Unit = {
      super.apply { () =>
        if (isTestInstanceSharded) {
          testFun()
        }
      }
    }
  }

  override protected def test(testName: String, testTags: Tag*): ResultOfTestInvocation = {
    new ShardResultOfTestInvocation(testName, testTags: _*)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (isTestInstanceSharded) {
      // trigger loading of orders table family
      // TestOracleHive.sql("use tpch")
      TestOracleHive.sql("describe tpch.orders").collect()
    }
  }

  override def afterAll(): Unit = {
    if (isTestInstanceSharded) {
      TestOracleHive.sql("use tpch")
    } else {
      super.afterAll()
    }
  }
}
