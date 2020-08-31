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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.oracle.operators.OraTableScanValidator.ScanDetails
import org.apache.spark.sql.types.Decimal

// scalastyle:off println
// scalastyle:off line.size.limit
class BasicExplainTest extends AbstractTest with PlanTestHelpers {

  /**
   * Notes:
   * - use [[PlanTestHelpers.showOraScans]] to get code for ScanDetails construction.
   */
  private def validateAndExplain(qry: String, planScans: Map[String, ScanDetails]): Unit = {
    validateOraScans(qry, planScans)
    showPlan(qry)
  }

  private def dumpScanDetails(qry: String): Unit = {
    showOraScans(qry, System.out)
  }

  def runQry(qry: String, planScans: Map[String, ScanDetails]): Unit = {
    if (planScans.isEmpty) {
      dumpScanDetails(qry)
    } else {
      // TODO recreate ScanDetails
      // validateAndExplain(qry, planScans)
      dumpScanDetails(qry)
    }
  }

  test("select") { td =>
    runQry(
      """
        |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK
        |from store_sales""".stripMargin,
      Map(("TPCDS.STORE_SALES" -> ScanDetails(
        List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_SOLD_DATE_SK"),
        None,
        List(),
        None,
        List()
      ))))
  }

  test("partFilter") { td =>
    runQry(
      """
        |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK * 5 - 50
        |from store_sales
        |where SS_SOLD_DATE_SK > 2451058
        |""".stripMargin,
      Map(("TPCDS.STORE_SALES" -> ScanDetails(
        List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_SOLD_DATE_SK"),
        None,
        List(),
        Some("(SS_SOLD_DATE_SK IS NOT NULL AND (SS_SOLD_DATE_SK > ?))"),
        List(Literal(Decimal(2451058.000000000000000000, 38, 18)))
      ))))
  }

  test("dataFilter") { td =>
    runQry(
      """
        |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK * 5 - 50
        |from store_sales
        |where SS_LIST_PRICE > 0
        |""".stripMargin,
      Map(("TPCDS.STORE_SALES" -> ScanDetails(
        List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_LIST_PRICE", "SS_SOLD_DATE_SK"),
        Some("(SS_LIST_PRICE IS NOT NULL AND (SS_LIST_PRICE > ?))"),
        List(Literal(Decimal(0E-18, 38, 18))),
        None,
        List()
      ))))
  }

  test("partAndDataFilter") { td =>
    runQry(
      """
              |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK * 5 - 50
              |from store_sales
              |where SS_LIST_PRICE > 0 and  SS_QUANTITY > 0 and SS_SOLD_DATE_SK > 2451058
              |""".stripMargin,
      Map(("TPCDS.STORE_SALES" -> ScanDetails(
        List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_QUANTITY", "SS_LIST_PRICE", "SS_SOLD_DATE_SK"),
        Some("(((SS_LIST_PRICE IS NOT NULL AND SS_QUANTITY IS NOT NULL) AND (SS_LIST_PRICE > ?)) AND (SS_QUANTITY > ?))"),
        List(Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(0E-18, 38, 18))),
        Some("(SS_SOLD_DATE_SK IS NOT NULL AND (SS_SOLD_DATE_SK > ?))"),
        List(Literal(Decimal(2451058.000000000000000000, 38, 18)))
      ))))
  }

  test("join") { td =>
    runQry(
      """
               |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK * 5 - 50, C_FIRST_NAME
               |from store_sales, customer
               |where SS_LIST_PRICE > 0 and  SS_QUANTITY > 0 and SS_SOLD_DATE_SK > 2451058
               |      and SS_CUSTOMER_SK = C_CUSTOMER_SK
               |      and C_BIRTH_YEAR > 2000
               |""".stripMargin,
      Map(("TPCDS.STORE_SALES" -> ScanDetails(
        List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_QUANTITY", "SS_LIST_PRICE", "SS_SOLD_DATE_SK"),
        Some("((((SS_LIST_PRICE IS NOT NULL AND SS_QUANTITY IS NOT NULL) AND (SS_LIST_PRICE > ?)) AND (SS_QUANTITY > ?)) AND SS_CUSTOMER_SK IS NOT NULL)"),
        List(Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(0E-18, 38, 18))),
        Some("(SS_SOLD_DATE_SK IS NOT NULL AND (SS_SOLD_DATE_SK > ?))"),
        List(Literal(Decimal(2451058.000000000000000000, 38, 18)))
      )),("TPCDS.CUSTOMER" -> ScanDetails(
        List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_BIRTH_YEAR"),
        Some("(C_BIRTH_YEAR IS NOT NULL AND (C_BIRTH_YEAR > ?))"),
        List(Literal(Decimal(2000.000000000000000000, 38, 18))),
        None,
        List()
      ))))
  }

}
