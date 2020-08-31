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

package org.apache.spark.sql.connector.catalog.oracle

import org.apache.spark.sql.oracle.AbstractTest

/*
Use to populate the cache for test env
run with params:
-Dspark.oracle.test.db_instance=scale1_tpcds
-Dspark.oracle.test.db_wallet_loc=/Users/hbutani/oracle/wallet_mammoth

Set Conf
.set("spark.sql.catalog.oracle.use_metadata_cache", "false")

Delete the contents of the metadata_cache folder
 */
class LoadMetadataCache
    extends AbstractTest
    with OraMetadataMgrInternalTest
    with TestMetadataValidation {

  test("populateMetadataCache") { td =>
    for ((ns, tbls) <- catalogTableMap if loadOraSchemaForTests(ns);
         tbl <- tbls) {
      // scalastyle:off println
      val bldr = new StringBuilder
      if (loadTableForTests(tbl)) {
        val oTbl = mdMgr.oraTable(ns, tbl)
        if (mdMgr.cache_only) {
          validate(oTbl)
        }
        oTbl.dump(bldr)
        println(bldr)
      }
    }
  }

}
