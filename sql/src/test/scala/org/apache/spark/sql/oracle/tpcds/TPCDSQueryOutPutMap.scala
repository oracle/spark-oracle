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

package org.apache.spark.sql.oracle.tpcds

import java.sql.Date

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.oracle.operators.OraTableScanValidator.ScanDetails
import org.apache.spark.sql.types.Decimal

object TPCDSQueryOutPutMap {

  // scalastyle:off line.size.limit
  private def q1 = Map(("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_CUSTOMER_SK", "SR_STORE_SK", "SR_RETURN_AMT", "SR_RETURNED_DATE_SK"),
    Some("(SR_STORE_SK IS NOT NULL AND SR_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("SR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_CUSTOMER_SK", "SR_STORE_SK", "SR_RETURN_AMT", "SR_RETURNED_DATE_SK"),
    Some("SR_STORE_SK IS NOT NULL"),
    List(),
    Some("(SR_RETURNED_DATE_SK IS NOT NULL AND SR_RETURNED_DATE_SK IS NOT NULL)"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STATE"),
    Some("(S_STATE IS NOT NULL AND (S_STATE = ?))"),
    List(Literal("TN")),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID"),
    None,
    List(),
    None,
    List()
  )))


  private def q2 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_WEEK_SEQ", "D_DAY_NAME"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_WEEK_SEQ", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR = ?)) AND D_WEEK_SEQ IS NOT NULL)"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_WEEK_SEQ", "D_DAY_NAME"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_WEEK_SEQ", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR = ?)) AND D_WEEK_SEQ IS NOT NULL)"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q3 = Map(("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(D_MOY IS NOT NULL AND (D_MOY = ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_BRAND", "I_MANUFACT_ID"),
    Some("(I_MANUFACT_ID IS NOT NULL AND (I_MANUFACT_ID = ?))"),
    List(Literal(Decimal(128.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q4 = Map(("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_EXT_DISCOUNT_AMT", "SS_EXT_SALES_PRICE", "SS_EXT_WHOLESALE_COST", "SS_EXT_LIST_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_EXT_DISCOUNT_AMT", "SS_EXT_SALES_PRICE", "SS_EXT_WHOLESALE_COST", "SS_EXT_LIST_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_EXT_DISCOUNT_AMT", "CS_EXT_SALES_PRICE", "CS_EXT_WHOLESALE_COST", "CS_EXT_LIST_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_EXT_DISCOUNT_AMT", "CS_EXT_SALES_PRICE", "CS_EXT_WHOLESALE_COST", "CS_EXT_LIST_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_CUSTOMER_SK", "WS_EXT_DISCOUNT_AMT", "WS_EXT_SALES_PRICE", "WS_EXT_WHOLESALE_COST", "WS_EXT_LIST_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_CUSTOMER_SK", "WS_EXT_DISCOUNT_AMT", "WS_EXT_SALES_PRICE", "WS_EXT_WHOLESALE_COST", "WS_EXT_LIST_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q5 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_STORE_SK", "SS_EXT_SALES_PRICE", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_STORE_SK", "SR_RETURN_AMT", "SR_NET_LOSS", "SR_RETURNED_DATE_SK"),
    Some("SR_STORE_SK IS NOT NULL"),
    List(),
    Some("SR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-06"))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_CATALOG_PAGE_SK", "CS_EXT_SALES_PRICE", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    Some("CS_CATALOG_PAGE_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_CATALOG_PAGE_SK", "CR_RETURN_AMOUNT", "CR_NET_LOSS", "CR_RETURNED_DATE_SK"),
    Some("CR_CATALOG_PAGE_SK IS NOT NULL"),
    List(),
    Some("CR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-06"))),
    None,
    List()
  )),("TPCDS.CATALOG_PAGE" -> ScanDetails(
    List("CP_CATALOG_PAGE_SK", "CP_CATALOG_PAGE_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_WEB_SITE_SK", "WS_EXT_SALES_PRICE", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    Some("WS_WEB_SITE_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ITEM_SK", "WR_ORDER_NUMBER", "WR_RETURN_AMT", "WR_NET_LOSS", "WR_RETURNED_DATE_SK"),
    None,
    List(),
    Some("WR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_WEB_SITE_SK", "WS_ORDER_NUMBER"),
    Some("WS_WEB_SITE_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-06"))),
    None,
    List()
  )),("TPCDS.WEB_SITE" -> ScanDetails(
    List("WEB_SITE_SK", "WEB_SITE_ID"),
    None,
    List(),
    None,
    List()
  )))


  private def q6 = Map(("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("D_MONTH_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CURRENT_PRICE", "I_CATEGORY"),
    Some("I_CURRENT_PRICE IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_CURRENT_PRICE", "I_CATEGORY"),
    Some("I_CATEGORY IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q7 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CDEMO_SK", "SS_PROMO_SK", "SS_QUANTITY", "SS_LIST_PRICE", "SS_SALES_PRICE", "SS_COUPON_AMT", "SS_SOLD_DATE_SK"),
    Some("(SS_CDEMO_SK IS NOT NULL AND SS_PROMO_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_GENDER", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS"),
    Some("(((((CD_GENDER IS NOT NULL AND CD_MARITAL_STATUS IS NOT NULL) AND CD_EDUCATION_STATUS IS NOT NULL) AND (CD_GENDER = ?)) AND (CD_MARITAL_STATUS = ?)) AND (CD_EDUCATION_STATUS = ?))"),
    List(Literal("M"), Literal("S"), Literal("College")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK", "P_CHANNEL_EMAIL", "P_CHANNEL_EVENT"),
    Some("((P_CHANNEL_EMAIL = ?) OR (P_CHANNEL_EVENT = ?))"),
    List(Literal("N"), Literal("N")),
    None,
    List()
  )))


  private def q8 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_STORE_SK", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_ZIP"),
    Some("S_ZIP IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CURRENT_ADDR_SK", "C_PREFERRED_CUST_FLAG"),
    Some("((C_PREFERRED_CUST_FLAG IS NOT NULL AND (C_PREFERRED_CUST_FLAG = ?)) AND C_CURRENT_ADDR_SK IS NOT NULL)"),
    List(Literal("Y")),
    None,
    List()
  )))


  private def q9 = Map(("TPCDS.REASON" -> ScanDetails(
    List("R_REASON_SK"),
    Some("(R_REASON_SK = ?)"),
    List(Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q10 = Map(("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_CDEMO_SK", "C_CURRENT_ADDR_SK"),
    Some("(C_CURRENT_ADDR_SK IS NOT NULL AND C_CURRENT_CDEMO_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_CDEMO_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_PROMO_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_SALES_PRICE", "SS_EXT_DISCOUNT_AMT", "SS_EXT_SALES_PRICE", "SS_EXT_WHOLESALE_COST", "SS_EXT_LIST_PRICE", "SS_EXT_TAX", "SS_COUPON_AMT", "SS_NET_PAID", "SS_NET_PAID_INC_TAX", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("((((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY >= ?)) AND (D_MOY <= ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SOLD_TIME_SK", "WS_SHIP_DATE_SK", "WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_BILL_CDEMO_SK", "WS_BILL_HDEMO_SK", "WS_BILL_ADDR_SK", "WS_SHIP_CUSTOMER_SK", "WS_SHIP_CDEMO_SK", "WS_SHIP_HDEMO_SK", "WS_SHIP_ADDR_SK", "WS_WEB_PAGE_SK", "WS_WEB_SITE_SK", "WS_SHIP_MODE_SK", "WS_WAREHOUSE_SK", "WS_PROMO_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_WHOLESALE_COST", "WS_LIST_PRICE", "WS_SALES_PRICE", "WS_EXT_DISCOUNT_AMT", "WS_EXT_SALES_PRICE", "WS_EXT_WHOLESALE_COST", "WS_EXT_LIST_PRICE", "WS_EXT_TAX", "WS_COUPON_AMT", "WS_EXT_SHIP_COST", "WS_NET_PAID", "WS_NET_PAID_INC_TAX", "WS_NET_PAID_INC_SHIP", "WS_NET_PAID_INC_SHIP_TAX", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("((((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY >= ?)) AND (D_MOY <= ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SOLD_TIME_SK", "CS_SHIP_DATE_SK", "CS_BILL_CUSTOMER_SK", "CS_BILL_CDEMO_SK", "CS_BILL_HDEMO_SK", "CS_BILL_ADDR_SK", "CS_SHIP_CUSTOMER_SK", "CS_SHIP_CDEMO_SK", "CS_SHIP_HDEMO_SK", "CS_SHIP_ADDR_SK", "CS_CALL_CENTER_SK", "CS_CATALOG_PAGE_SK", "CS_SHIP_MODE_SK", "CS_WAREHOUSE_SK", "CS_ITEM_SK", "CS_PROMO_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_WHOLESALE_COST", "CS_LIST_PRICE", "CS_SALES_PRICE", "CS_EXT_DISCOUNT_AMT", "CS_EXT_SALES_PRICE", "CS_EXT_WHOLESALE_COST", "CS_EXT_LIST_PRICE", "CS_EXT_TAX", "CS_COUPON_AMT", "CS_EXT_SHIP_COST", "CS_NET_PAID", "CS_NET_PAID_INC_TAX", "CS_NET_PAID_INC_SHIP", "CS_NET_PAID_INC_SHIP_TAX", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("((((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY >= ?)) AND (D_MOY <= ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY"),
    Some("CA_COUNTY IN ( ?, ?, ?, ?, ? )"),
    List(Literal("Rush County"), Literal("Toole County"), Literal("Jefferson County"), Literal("Dona Ana County"), Literal("La Porte County")),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_GENDER", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS", "CD_PURCHASE_ESTIMATE", "CD_CREDIT_RATING", "CD_DEP_COUNT", "CD_DEP_EMPLOYED_COUNT", "CD_DEP_COLLEGE_COUNT"),
    None,
    List(),
    None,
    List()
  )))


  private def q11 = Map(("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_EXT_DISCOUNT_AMT", "SS_EXT_LIST_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_EXT_DISCOUNT_AMT", "SS_EXT_LIST_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_CUSTOMER_SK", "WS_EXT_DISCOUNT_AMT", "WS_EXT_LIST_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_CUSTOMER_SK", "WS_EXT_DISCOUNT_AMT", "WS_EXT_LIST_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q12 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_ITEM_DESC", "I_CURRENT_PRICE", "I_CLASS", "I_CATEGORY"),
    Some("I_CATEGORY IN ( ?, ?, ? )"),
    List(Literal("Sports"), Literal("Books"), Literal("Home")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("1999-02-22")), Literal(Date.valueOf("1999-03-24"))),
    None,
    List()
  )))


  private def q13 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CDEMO_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_QUANTITY", "SS_SALES_PRICE", "SS_EXT_SALES_PRICE", "SS_EXT_WHOLESALE_COST", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("(((((SS_STORE_SK IS NOT NULL AND SS_ADDR_SK IS NOT NULL) AND SS_CDEMO_SK IS NOT NULL) AND SS_HDEMO_SK IS NOT NULL) AND ((((SS_NET_PROFIT >= ?) AND (SS_NET_PROFIT <= ?)) OR ((SS_NET_PROFIT >= ?) AND (SS_NET_PROFIT <= ?))) OR ((SS_NET_PROFIT >= ?) AND (SS_NET_PROFIT <= ?)))) AND ((((SS_SALES_PRICE >= ?) AND (SS_SALES_PRICE <= ?)) OR ((SS_SALES_PRICE >= ?) AND (SS_SALES_PRICE <= ?))) OR ((SS_SALES_PRICE >= ?) AND (SS_SALES_PRICE <= ?))))"),
    List(Literal(Decimal(100.000000000000000000, 38, 18)), Literal(Decimal(200.000000000000000000, 38, 18)), Literal(Decimal(150.000000000000000000, 38, 18)), Literal(Decimal(300.000000000000000000, 38, 18)), Literal(Decimal(50.000000000000000000, 38, 18)), Literal(Decimal(250.000000000000000000, 38, 18)), Literal(Decimal(100.000000000000000000, 38, 18)), Literal(Decimal(150.000000000000000000, 38, 18)), Literal(Decimal(50.000000000000000000, 38, 18)), Literal(Decimal(100.000000000000000000, 38, 18)), Literal(Decimal(150.000000000000000000, 38, 18)), Literal(Decimal(200.000000000000000000, 38, 18))),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE", "CA_COUNTRY"),
    Some("((CA_COUNTRY IS NOT NULL AND (CA_COUNTRY = ?)) AND ((CA_STATE IN ( ?, ? ) OR CA_STATE IN ( ?, ?, ? )) OR CA_STATE IN ( ?, ?, ? )))"),
    List(Literal("United States"), Literal("TX"), Literal("OH"), Literal("OR"), Literal("NM"), Literal("KY"), Literal("VA"), Literal("TX"), Literal("MS")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS"),
    Some("((((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?)) OR ((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?))) OR ((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?)))"),
    List(Literal("M"), Literal("Advanced Degree"), Literal("S"), Literal("College"), Literal("W"), Literal("2 yr Degree")),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT"),
    Some("(((HD_DEP_COUNT = ?) OR (HD_DEP_COUNT = ?)) OR (HD_DEP_COUNT = ?))"),
    List(Literal(Decimal(3.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q14_1 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_QUANTITY", "SS_LIST_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(11.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_QUANTITY", "CS_LIST_PRICE", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(11.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_QUANTITY", "WS_LIST_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(11.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q14_2 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_QUANTITY", "SS_LIST_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_WEEK_SEQ"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_QUANTITY", "SS_LIST_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    Some("((I_BRAND_ID IS NOT NULL AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR >= ?)) AND (D_YEAR <= ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_WEEK_SEQ"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q15 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE", "CA_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q16 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SHIP_DATE_SK", "CS_SHIP_ADDR_SK", "CS_CALL_CENTER_SK", "CS_WAREHOUSE_SK", "CS_ORDER_NUMBER", "CS_EXT_SHIP_COST", "CS_NET_PROFIT"),
    Some("((CS_SHIP_DATE_SK IS NOT NULL AND CS_SHIP_ADDR_SK IS NOT NULL) AND CS_CALL_CENTER_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SOLD_TIME_SK", "CS_SHIP_DATE_SK", "CS_BILL_CUSTOMER_SK", "CS_BILL_CDEMO_SK", "CS_BILL_HDEMO_SK", "CS_BILL_ADDR_SK", "CS_SHIP_CUSTOMER_SK", "CS_SHIP_CDEMO_SK", "CS_SHIP_HDEMO_SK", "CS_SHIP_ADDR_SK", "CS_CALL_CENTER_SK", "CS_CATALOG_PAGE_SK", "CS_SHIP_MODE_SK", "CS_WAREHOUSE_SK", "CS_ITEM_SK", "CS_PROMO_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_WHOLESALE_COST", "CS_LIST_PRICE", "CS_SALES_PRICE", "CS_EXT_DISCOUNT_AMT", "CS_EXT_SALES_PRICE", "CS_EXT_WHOLESALE_COST", "CS_EXT_LIST_PRICE", "CS_EXT_TAX", "CS_COUPON_AMT", "CS_EXT_SHIP_COST", "CS_NET_PAID", "CS_NET_PAID_INC_TAX", "CS_NET_PAID_INC_SHIP", "CS_NET_PAID_INC_SHIP_TAX", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_RETURNED_TIME_SK", "CR_ITEM_SK", "CR_REFUNDED_CUSTOMER_SK", "CR_REFUNDED_CDEMO_SK", "CR_REFUNDED_HDEMO_SK", "CR_REFUNDED_ADDR_SK", "CR_RETURNING_CUSTOMER_SK", "CR_RETURNING_CDEMO_SK", "CR_RETURNING_HDEMO_SK", "CR_RETURNING_ADDR_SK", "CR_CALL_CENTER_SK", "CR_CATALOG_PAGE_SK", "CR_SHIP_MODE_SK", "CR_WAREHOUSE_SK", "CR_REASON_SK", "CR_ORDER_NUMBER", "CR_RETURN_QUANTITY", "CR_RETURN_AMOUNT", "CR_RETURN_TAX", "CR_RETURN_AMT_INC_TAX", "CR_FEE", "CR_RETURN_SHIP_COST", "CR_REFUNDED_CASH", "CR_REVERSED_CHARGE", "CR_STORE_CREDIT", "CR_NET_LOSS", "CR_RETURNED_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2002-02-01")), Literal(Date.valueOf("2002-04-02"))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("(CA_STATE IS NOT NULL AND (CA_STATE = ?))"),
    List(Literal("GA")),
    None,
    List()
  )),("TPCDS.CALL_CENTER" -> ScanDetails(
    List("CC_CALL_CENTER_SK", "CC_COUNTY"),
    Some("(CC_COUNTY IS NOT NULL AND (CC_COUNTY = ?))"),
    List(Literal("Williamson County")),
    None,
    List()
  )))


  private def q17 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_SOLD_DATE_SK"),
    Some("(SS_CUSTOMER_SK IS NOT NULL AND SS_STORE_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_CUSTOMER_SK", "SR_TICKET_NUMBER", "SR_RETURN_QUANTITY", "SR_RETURNED_DATE_SK"),
    Some("SR_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_ITEM_SK", "CS_QUANTITY", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_QUARTER_NAME"),
    Some("(D_QUARTER_NAME IS NOT NULL AND (D_QUARTER_NAME = ?))"),
    List(Literal("2001Q1")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_QUARTER_NAME"),
    Some("D_QUARTER_NAME IN ( ?, ?, ? )"),
    List(Literal("2001Q1"), Literal("2001Q2"), Literal("2001Q3")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_QUARTER_NAME"),
    Some("D_QUARTER_NAME IN ( ?, ?, ? )"),
    List(Literal("2001Q1"), Literal("2001Q2"), Literal("2001Q3")),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_ITEM_DESC"),
    None,
    List(),
    None,
    List()
  )))


  private def q18 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_BILL_CDEMO_SK", "CS_ITEM_SK", "CS_QUANTITY", "CS_LIST_PRICE", "CS_SALES_PRICE", "CS_COUPON_AMT", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    Some("(CS_BILL_CDEMO_SK IS NOT NULL AND CS_BILL_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_GENDER", "CD_EDUCATION_STATUS", "CD_DEP_COUNT"),
    Some("(((CD_GENDER IS NOT NULL AND CD_EDUCATION_STATUS IS NOT NULL) AND (CD_GENDER = ?)) AND (CD_EDUCATION_STATUS = ?))"),
    List(Literal("F"), Literal("Unknown")),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_CDEMO_SK", "C_CURRENT_ADDR_SK", "C_BIRTH_MONTH", "C_BIRTH_YEAR"),
    Some("((C_BIRTH_MONTH IN ( ?, ?, ?, ?, ?, ? ) AND C_CURRENT_CDEMO_SK IS NOT NULL) AND C_CURRENT_ADDR_SK IS NOT NULL)"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(8.000000000000000000, 38, 18)), Literal(Decimal(9.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY", "CA_STATE", "CA_COUNTRY"),
    Some("CA_STATE IN ( ?, ?, ?, ?, ?, ? )"),
    List(Literal("MS"), Literal("IN"), Literal("ND"), Literal("OK"), Literal("NM"), Literal("VA")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )))


  private def q19 = Map(("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("(SS_CUSTOMER_SK IS NOT NULL AND SS_STORE_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_BRAND", "I_MANUFACT_ID", "I_MANUFACT", "I_MANAGER_ID"),
    Some("(I_MANAGER_ID IS NOT NULL AND (I_MANAGER_ID = ?))"),
    List(Literal(Decimal(8.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_ZIP"),
    Some("CA_ZIP IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_ZIP"),
    Some("S_ZIP IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q20 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_ITEM_DESC", "I_CURRENT_PRICE", "I_CLASS", "I_CATEGORY"),
    Some("I_CATEGORY IN ( ?, ?, ? )"),
    List(Literal("Sports"), Literal("Books"), Literal("Home")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("1999-02-22")), Literal(Date.valueOf("1999-03-24"))),
    None,
    List()
  )))


  private def q21 = Map(("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_WAREHOUSE_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_CURRENT_PRICE"),
    Some("((I_CURRENT_PRICE IS NOT NULL AND (I_CURRENT_PRICE >= ?)) AND (I_CURRENT_PRICE <= ?))"),
    List(Literal(Decimal(0.990000000000000000, 38, 18)), Literal(Decimal(1.490000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-02-10")), Literal(Date.valueOf("2000-04-10"))),
    None,
    List()
  )))


  private def q22 = Map(("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CLASS", "I_CATEGORY", "I_PRODUCT_NAME"),
    None,
    List(),
    None,
    List()
  )))


  private def q23_1 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_ITEM_SK", "CS_QUANTITY", "CS_LIST_PRICE", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_YEAR"),
    Some("D_YEAR IN ( ?, ?, ?, ? )"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(2003.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_DESC"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_QUANTITY", "SS_SALES_PRICE"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_QUANTITY", "WS_LIST_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_YEAR"),
    Some("D_YEAR IN ( ?, ?, ?, ? )"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(2003.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_DESC"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_QUANTITY", "SS_SALES_PRICE"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q23_2 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_ITEM_SK", "CS_QUANTITY", "CS_LIST_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_YEAR"),
    Some("D_YEAR IN ( ?, ?, ?, ? )"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(2003.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_DESC"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_QUANTITY", "SS_SALES_PRICE"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_QUANTITY", "SS_SALES_PRICE"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_QUANTITY", "WS_LIST_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_YEAR"),
    Some("D_YEAR IN ( ?, ?, ?, ? )"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(2003.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_DESC"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_QUANTITY", "SS_SALES_PRICE"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_QUANTITY", "SS_SALES_PRICE"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q24_1 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_NET_PAID"),
    Some("(SS_STORE_SK IS NOT NULL AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_MARKET_ID", "S_STATE", "S_ZIP"),
    Some("((S_MARKET_ID IS NOT NULL AND (S_MARKET_ID = ?)) AND S_ZIP IS NOT NULL)"),
    List(Literal(Decimal(8.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CURRENT_PRICE", "I_SIZE", "I_COLOR", "I_UNITS", "I_MANAGER_ID"),
    Some("(I_COLOR IS NOT NULL AND (I_COLOR = ?))"),
    List(Literal("pale")),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK", "C_FIRST_NAME", "C_LAST_NAME", "C_BIRTH_COUNTRY"),
    Some("(C_CURRENT_ADDR_SK IS NOT NULL AND C_BIRTH_COUNTRY IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE", "CA_ZIP", "CA_COUNTRY"),
    Some("(CA_COUNTRY IS NOT NULL AND CA_ZIP IS NOT NULL)"),
    List(),
    None,
    List()
  )))


  private def q24_2 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_NET_PAID"),
    Some("(SS_STORE_SK IS NOT NULL AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_MARKET_ID", "S_STATE", "S_ZIP"),
    Some("((S_MARKET_ID IS NOT NULL AND (S_MARKET_ID = ?)) AND S_ZIP IS NOT NULL)"),
    List(Literal(Decimal(8.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CURRENT_PRICE", "I_SIZE", "I_COLOR", "I_UNITS", "I_MANAGER_ID"),
    Some("(I_COLOR IS NOT NULL AND (I_COLOR = ?))"),
    List(Literal("chiffon")),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK", "C_FIRST_NAME", "C_LAST_NAME", "C_BIRTH_COUNTRY"),
    Some("(C_CURRENT_ADDR_SK IS NOT NULL AND C_BIRTH_COUNTRY IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE", "CA_ZIP", "CA_COUNTRY"),
    Some("(CA_COUNTRY IS NOT NULL AND CA_ZIP IS NOT NULL)"),
    List(),
    None,
    List()
  )))


  private def q25 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("(SS_CUSTOMER_SK IS NOT NULL AND SS_STORE_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_CUSTOMER_SK", "SR_TICKET_NUMBER", "SR_NET_LOSS", "SR_RETURNED_DATE_SK"),
    Some("SR_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_ITEM_SK", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("((((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY >= ?)) AND (D_MOY <= ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(10.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("((((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY >= ?)) AND (D_MOY <= ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(10.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_ID", "S_STORE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_ITEM_DESC"),
    None,
    List(),
    None,
    List()
  )))


  private def q26 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CDEMO_SK", "CS_ITEM_SK", "CS_PROMO_SK", "CS_QUANTITY", "CS_LIST_PRICE", "CS_SALES_PRICE", "CS_COUPON_AMT", "CS_SOLD_DATE_SK"),
    Some("(CS_BILL_CDEMO_SK IS NOT NULL AND CS_PROMO_SK IS NOT NULL)"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_GENDER", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS"),
    Some("(((((CD_GENDER IS NOT NULL AND CD_MARITAL_STATUS IS NOT NULL) AND CD_EDUCATION_STATUS IS NOT NULL) AND (CD_GENDER = ?)) AND (CD_MARITAL_STATUS = ?)) AND (CD_EDUCATION_STATUS = ?))"),
    List(Literal("M"), Literal("S"), Literal("College")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK", "P_CHANNEL_EMAIL", "P_CHANNEL_EVENT"),
    Some("((P_CHANNEL_EMAIL = ?) OR (P_CHANNEL_EVENT = ?))"),
    List(Literal("N"), Literal("N")),
    None,
    List()
  )))


  private def q27 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CDEMO_SK", "SS_STORE_SK", "SS_QUANTITY", "SS_LIST_PRICE", "SS_SALES_PRICE", "SS_COUPON_AMT", "SS_SOLD_DATE_SK"),
    Some("(SS_CDEMO_SK IS NOT NULL AND SS_STORE_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_GENDER", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS"),
    Some("(((((CD_GENDER IS NOT NULL AND CD_MARITAL_STATUS IS NOT NULL) AND CD_EDUCATION_STATUS IS NOT NULL) AND (CD_GENDER = ?)) AND (CD_MARITAL_STATUS = ?)) AND (CD_EDUCATION_STATUS = ?))"),
    List(Literal("M"), Literal("S"), Literal("College")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STATE"),
    Some("(S_STATE IS NOT NULL AND (S_STATE = ?))"),
    List(Literal("TN")),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )))


  private def q28 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_COUPON_AMT"),
    Some("(((SS_QUANTITY IS NOT NULL AND (SS_QUANTITY >= ?)) AND (SS_QUANTITY <= ?)) AND ((((SS_LIST_PRICE >= ?) AND (SS_LIST_PRICE <= ?)) OR ((SS_COUPON_AMT >= ?) AND (SS_COUPON_AMT <= ?))) OR ((SS_WHOLESALE_COST >= ?) AND (SS_WHOLESALE_COST <= ?))))"),
    List(Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(5.000000000000000000, 38, 18)), Literal(Decimal(8.000000000000000000, 38, 18)), Literal(Decimal(18.000000000000000000, 38, 18)), Literal(Decimal(459.000000000000000000, 38, 18)), Literal(Decimal(1459.000000000000000000, 38, 18)), Literal(Decimal(57.000000000000000000, 38, 18)), Literal(Decimal(77.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_COUPON_AMT"),
    Some("(((SS_QUANTITY IS NOT NULL AND (SS_QUANTITY >= ?)) AND (SS_QUANTITY <= ?)) AND ((((SS_LIST_PRICE >= ?) AND (SS_LIST_PRICE <= ?)) OR ((SS_COUPON_AMT >= ?) AND (SS_COUPON_AMT <= ?))) OR ((SS_WHOLESALE_COST >= ?) AND (SS_WHOLESALE_COST <= ?))))"),
    List(Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(10.000000000000000000, 38, 18)), Literal(Decimal(90.000000000000000000, 38, 18)), Literal(Decimal(100.000000000000000000, 38, 18)), Literal(Decimal(2323.000000000000000000, 38, 18)), Literal(Decimal(3323.000000000000000000, 38, 18)), Literal(Decimal(31.000000000000000000, 38, 18)), Literal(Decimal(51.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_COUPON_AMT"),
    Some("(((SS_QUANTITY IS NOT NULL AND (SS_QUANTITY >= ?)) AND (SS_QUANTITY <= ?)) AND ((((SS_LIST_PRICE >= ?) AND (SS_LIST_PRICE <= ?)) OR ((SS_COUPON_AMT >= ?) AND (SS_COUPON_AMT <= ?))) OR ((SS_WHOLESALE_COST >= ?) AND (SS_WHOLESALE_COST <= ?))))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(15.000000000000000000, 38, 18)), Literal(Decimal(142.000000000000000000, 38, 18)), Literal(Decimal(152.000000000000000000, 38, 18)), Literal(Decimal(12214.000000000000000000, 38, 18)), Literal(Decimal(13214.000000000000000000, 38, 18)), Literal(Decimal(79.000000000000000000, 38, 18)), Literal(Decimal(99.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_COUPON_AMT"),
    Some("(((SS_QUANTITY IS NOT NULL AND (SS_QUANTITY >= ?)) AND (SS_QUANTITY <= ?)) AND ((((SS_LIST_PRICE >= ?) AND (SS_LIST_PRICE <= ?)) OR ((SS_COUPON_AMT >= ?) AND (SS_COUPON_AMT <= ?))) OR ((SS_WHOLESALE_COST >= ?) AND (SS_WHOLESALE_COST <= ?))))"),
    List(Literal(Decimal(16.000000000000000000, 38, 18)), Literal(Decimal(20.000000000000000000, 38, 18)), Literal(Decimal(135.000000000000000000, 38, 18)), Literal(Decimal(145.000000000000000000, 38, 18)), Literal(Decimal(6071.000000000000000000, 38, 18)), Literal(Decimal(7071.000000000000000000, 38, 18)), Literal(Decimal(38.000000000000000000, 38, 18)), Literal(Decimal(58.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_COUPON_AMT"),
    Some("(((SS_QUANTITY IS NOT NULL AND (SS_QUANTITY >= ?)) AND (SS_QUANTITY <= ?)) AND ((((SS_LIST_PRICE >= ?) AND (SS_LIST_PRICE <= ?)) OR ((SS_COUPON_AMT >= ?) AND (SS_COUPON_AMT <= ?))) OR ((SS_WHOLESALE_COST >= ?) AND (SS_WHOLESALE_COST <= ?))))"),
    List(Literal(Decimal(21.000000000000000000, 38, 18)), Literal(Decimal(25.000000000000000000, 38, 18)), Literal(Decimal(122.000000000000000000, 38, 18)), Literal(Decimal(132.000000000000000000, 38, 18)), Literal(Decimal(836.000000000000000000, 38, 18)), Literal(Decimal(1836.000000000000000000, 38, 18)), Literal(Decimal(17.000000000000000000, 38, 18)), Literal(Decimal(37.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_COUPON_AMT"),
    Some("(((SS_QUANTITY IS NOT NULL AND (SS_QUANTITY >= ?)) AND (SS_QUANTITY <= ?)) AND ((((SS_LIST_PRICE >= ?) AND (SS_LIST_PRICE <= ?)) OR ((SS_COUPON_AMT >= ?) AND (SS_COUPON_AMT <= ?))) OR ((SS_WHOLESALE_COST >= ?) AND (SS_WHOLESALE_COST <= ?))))"),
    List(Literal(Decimal(26.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18)), Literal(Decimal(154.000000000000000000, 38, 18)), Literal(Decimal(164.000000000000000000, 38, 18)), Literal(Decimal(7326.000000000000000000, 38, 18)), Literal(Decimal(8326.000000000000000000, 38, 18)), Literal(Decimal(7.000000000000000000, 38, 18)), Literal(Decimal(27.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q29 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_SOLD_DATE_SK"),
    Some("(SS_CUSTOMER_SK IS NOT NULL AND SS_STORE_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_CUSTOMER_SK", "SR_TICKET_NUMBER", "SR_RETURN_QUANTITY", "SR_RETURNED_DATE_SK"),
    Some("SR_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_ITEM_SK", "CS_QUANTITY", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(9.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("((((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY >= ?)) AND (D_MOY <= ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(9.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("D_YEAR IN ( ?, ?, ? )"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_ID", "S_STORE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_ITEM_DESC"),
    None,
    List(),
    None,
    List()
  )))


  private def q30 = Map(("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_RETURNING_CUSTOMER_SK", "WR_RETURNING_ADDR_SK", "WR_RETURN_AMT", "WR_RETURNED_DATE_SK"),
    Some("(WR_RETURNING_ADDR_SK IS NOT NULL AND WR_RETURNING_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("WR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("CA_STATE IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_RETURNING_CUSTOMER_SK", "WR_RETURNING_ADDR_SK", "WR_RETURN_AMT", "WR_RETURNED_DATE_SK"),
    Some("WR_RETURNING_ADDR_SK IS NOT NULL"),
    List(),
    Some("WR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("CA_STATE IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_CURRENT_ADDR_SK", "C_SALUTATION", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG", "C_BIRTH_DAY", "C_BIRTH_MONTH", "C_BIRTH_YEAR", "C_BIRTH_COUNTRY", "C_LOGIN", "C_EMAIL_ADDRESS", "C_LAST_REVIEW_DATE_SK"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("(CA_STATE IS NOT NULL AND (CA_STATE = ?))"),
    List(Literal("GA")),
    None,
    List()
  )))


  private def q31 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ADDR_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_ADDR_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY"),
    Some("CA_COUNTY IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ADDR_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_ADDR_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY"),
    Some("CA_COUNTY IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ADDR_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_ADDR_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(3.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY"),
    Some("CA_COUNTY IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_ADDR_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY"),
    Some("CA_COUNTY IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_ADDR_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY"),
    Some("CA_COUNTY IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_ADDR_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(3.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY"),
    Some("CA_COUNTY IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q32 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_EXT_DISCOUNT_AMT", "CS_SOLD_DATE_SK"),
    Some("CS_EXT_DISCOUNT_AMT IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_MANUFACT_ID"),
    Some("(I_MANUFACT_ID IS NOT NULL AND (I_MANUFACT_ID = ?))"),
    List(Literal(Decimal(977.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_EXT_DISCOUNT_AMT", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-01-27")), Literal(Date.valueOf("2000-04-26"))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-01-27")), Literal(Date.valueOf("2000-04-26"))),
    None,
    List()
  )))


  private def q33 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_ADDR_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_ADDR_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(5.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_MANUFACT_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_CATEGORY", "I_MANUFACT_ID"),
    Some("(I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?))"),
    List(Literal("Electronics")),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_ADDR_SK", "CS_ITEM_SK", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(5.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_MANUFACT_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_CATEGORY", "I_MANUFACT_ID"),
    Some("(I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?))"),
    List(Literal("Electronics")),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_BILL_ADDR_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(5.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_MANUFACT_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_CATEGORY", "I_MANUFACT_ID"),
    Some("(I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?))"),
    List(Literal("Electronics")),
    None,
    List()
  )))


  private def q34 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_HDEMO_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_SOLD_DATE_SK"),
    Some("((SS_STORE_SK IS NOT NULL AND SS_HDEMO_SK IS NOT NULL) AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_DOM"),
    Some("((((D_DOM >= ?) AND (D_DOM <= ?)) OR ((D_DOM >= ?) AND (D_DOM <= ?))) AND D_YEAR IN ( ?, ?, ? ))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(3.000000000000000000, 38, 18)), Literal(Decimal(25.000000000000000000, 38, 18)), Literal(Decimal(28.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_COUNTY"),
    Some("(S_COUNTY IS NOT NULL AND (S_COUNTY = ?))"),
    List(Literal("Williamson County")),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_BUY_POTENTIAL", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((HD_VEHICLE_COUNT IS NOT NULL AND ((HD_BUY_POTENTIAL = ?) OR (HD_BUY_POTENTIAL = ?))) AND (HD_VEHICLE_COUNT > ?))"),
    List(Literal(">10000"), Literal("Unknown"), Literal(Decimal(0E-18, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_SALUTATION", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG"),
    None,
    List(),
    None,
    List()
  )))


  private def q35 = Map(("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_CDEMO_SK", "C_CURRENT_ADDR_SK"),
    Some("(C_CURRENT_ADDR_SK IS NOT NULL AND C_CURRENT_CDEMO_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_CDEMO_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_PROMO_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_SALES_PRICE", "SS_EXT_DISCOUNT_AMT", "SS_EXT_SALES_PRICE", "SS_EXT_WHOLESALE_COST", "SS_EXT_LIST_PRICE", "SS_EXT_TAX", "SS_COUPON_AMT", "SS_NET_PAID", "SS_NET_PAID_INC_TAX", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("(((D_YEAR IS NOT NULL AND D_QOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_QOY < ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SOLD_TIME_SK", "WS_SHIP_DATE_SK", "WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_BILL_CDEMO_SK", "WS_BILL_HDEMO_SK", "WS_BILL_ADDR_SK", "WS_SHIP_CUSTOMER_SK", "WS_SHIP_CDEMO_SK", "WS_SHIP_HDEMO_SK", "WS_SHIP_ADDR_SK", "WS_WEB_PAGE_SK", "WS_WEB_SITE_SK", "WS_SHIP_MODE_SK", "WS_WAREHOUSE_SK", "WS_PROMO_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_WHOLESALE_COST", "WS_LIST_PRICE", "WS_SALES_PRICE", "WS_EXT_DISCOUNT_AMT", "WS_EXT_SALES_PRICE", "WS_EXT_WHOLESALE_COST", "WS_EXT_LIST_PRICE", "WS_EXT_TAX", "WS_COUPON_AMT", "WS_EXT_SHIP_COST", "WS_NET_PAID", "WS_NET_PAID_INC_TAX", "WS_NET_PAID_INC_SHIP", "WS_NET_PAID_INC_SHIP_TAX", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("(((D_YEAR IS NOT NULL AND D_QOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_QOY < ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SOLD_TIME_SK", "CS_SHIP_DATE_SK", "CS_BILL_CUSTOMER_SK", "CS_BILL_CDEMO_SK", "CS_BILL_HDEMO_SK", "CS_BILL_ADDR_SK", "CS_SHIP_CUSTOMER_SK", "CS_SHIP_CDEMO_SK", "CS_SHIP_HDEMO_SK", "CS_SHIP_ADDR_SK", "CS_CALL_CENTER_SK", "CS_CATALOG_PAGE_SK", "CS_SHIP_MODE_SK", "CS_WAREHOUSE_SK", "CS_ITEM_SK", "CS_PROMO_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_WHOLESALE_COST", "CS_LIST_PRICE", "CS_SALES_PRICE", "CS_EXT_DISCOUNT_AMT", "CS_EXT_SALES_PRICE", "CS_EXT_WHOLESALE_COST", "CS_EXT_LIST_PRICE", "CS_EXT_TAX", "CS_COUPON_AMT", "CS_EXT_SHIP_COST", "CS_NET_PAID", "CS_NET_PAID_INC_TAX", "CS_NET_PAID_INC_SHIP", "CS_NET_PAID_INC_SHIP_TAX", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("(((D_YEAR IS NOT NULL AND D_QOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_QOY < ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_GENDER", "CD_MARITAL_STATUS", "CD_DEP_COUNT", "CD_DEP_EMPLOYED_COUNT", "CD_DEP_COLLEGE_COUNT"),
    None,
    List(),
    None,
    List()
  )))


  private def q36 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_EXT_SALES_PRICE", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CLASS", "I_CATEGORY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STATE"),
    Some("(S_STATE IS NOT NULL AND (S_STATE = ?))"),
    List(Literal("TN")),
    None,
    List()
  )))


  private def q37 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_ITEM_DESC", "I_CURRENT_PRICE", "I_MANUFACT_ID"),
    Some("(((I_CURRENT_PRICE IS NOT NULL AND (I_CURRENT_PRICE >= ?)) AND (I_CURRENT_PRICE <= ?)) AND I_MANUFACT_ID IN ( ?, ?, ?, ? ))"),
    List(Literal(Decimal(68.000000000000000000, 38, 18)), Literal(Decimal(98.000000000000000000, 38, 18)), Literal(Decimal(677.000000000000000000, 38, 18)), Literal(Decimal(940.000000000000000000, 38, 18)), Literal(Decimal(694.000000000000000000, 38, 18)), Literal(Decimal(808.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    Some("((INV_QUANTITY_ON_HAND IS NOT NULL AND (INV_QUANTITY_ON_HAND >= ?)) AND (INV_QUANTITY_ON_HAND <= ?))"),
    List(Literal(Decimal(100.000000000000000000, 38, 18)), Literal(Decimal(500.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-02-01")), Literal(Date.valueOf("2000-04-01"))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK"),
    None,
    List(),
    None,
    List()
  )))


  private def q38 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_CUSTOMER_SK", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )))


  private def q39_1 = Map(("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_WAREHOUSE_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_WAREHOUSE_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q39_2 = Map(("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_WAREHOUSE_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_WAREHOUSE_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q40 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_WAREHOUSE_SK", "CS_ITEM_SK", "CS_ORDER_NUMBER", "CS_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_WAREHOUSE_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER", "CR_REFUNDED_CASH"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_STATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_CURRENT_PRICE"),
    Some("((I_CURRENT_PRICE IS NOT NULL AND (I_CURRENT_PRICE >= ?)) AND (I_CURRENT_PRICE <= ?))"),
    List(Literal(Decimal(0.990000000000000000, 38, 18)), Literal(Decimal(1.490000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-02-10")), Literal(Date.valueOf("2000-04-10"))),
    None,
    List()
  )))


  private def q41 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_MANUFACT_ID", "I_MANUFACT", "I_PRODUCT_NAME"),
    Some("(((I_MANUFACT_ID IS NOT NULL AND (I_MANUFACT_ID >= ?)) AND (I_MANUFACT_ID <= ?)) AND I_MANUFACT IS NOT NULL)"),
    List(Literal(Decimal(738.000000000000000000, 38, 18)), Literal(Decimal(778.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_CATEGORY", "I_MANUFACT", "I_SIZE", "I_COLOR", "I_UNITS"),
    Some("((((I_CATEGORY = ?) AND (((((I_COLOR = ?) OR (I_COLOR = ?)) AND ((I_UNITS = ?) OR (I_UNITS = ?))) AND ((I_SIZE = ?) OR (I_SIZE = ?))) OR ((((I_COLOR = ?) OR (I_COLOR = ?)) AND ((I_UNITS = ?) OR (I_UNITS = ?))) AND ((I_SIZE = ?) OR (I_SIZE = ?))))) OR ((I_CATEGORY = ?) AND (((((I_COLOR = ?) OR (I_COLOR = ?)) AND ((I_UNITS = ?) OR (I_UNITS = ?))) AND ((I_SIZE = ?) OR (I_SIZE = ?))) OR ((((I_COLOR = ?) OR (I_COLOR = ?)) AND ((I_UNITS = ?) OR (I_UNITS = ?))) AND ((I_SIZE = ?) OR (I_SIZE = ?)))))) OR (((I_CATEGORY = ?) AND (((((I_COLOR = ?) OR (I_COLOR = ?)) AND ((I_UNITS = ?) OR (I_UNITS = ?))) AND ((I_SIZE = ?) OR (I_SIZE = ?))) OR ((((I_COLOR = ?) OR (I_COLOR = ?)) AND ((I_UNITS = ?) OR (I_UNITS = ?))) AND ((I_SIZE = ?) OR (I_SIZE = ?))))) OR ((I_CATEGORY = ?) AND (((((I_COLOR = ?) OR (I_COLOR = ?)) AND ((I_UNITS = ?) OR (I_UNITS = ?))) AND ((I_SIZE = ?) OR (I_SIZE = ?))) OR ((((I_COLOR = ?) OR (I_COLOR = ?)) AND ((I_UNITS = ?) OR (I_UNITS = ?))) AND ((I_SIZE = ?) OR (I_SIZE = ?)))))))"),
    List(Literal("Women"), Literal("powder"), Literal("khaki"), Literal("Ounce"), Literal("Oz"), Literal("medium"), Literal("extra large"), Literal("brown"), Literal("honeydew"), Literal("Bunch"), Literal("Ton"), Literal("N/A"), Literal("small"), Literal("Men"), Literal("floral"), Literal("deep"), Literal("N/A"), Literal("Dozen"), Literal("petite"), Literal("large"), Literal("light"), Literal("cornflower"), Literal("Box"), Literal("Pound"), Literal("medium"), Literal("extra large"), Literal("Women"), Literal("midnight"), Literal("snow"), Literal("Pallet"), Literal("Gross"), Literal("medium"), Literal("extra large"), Literal("cyan"), Literal("papaya"), Literal("Cup"), Literal("Dram"), Literal("N/A"), Literal("small"), Literal("Men"), Literal("orange"), Literal("frosted"), Literal("Each"), Literal("Tbl"), Literal("petite"), Literal("large"), Literal("forest"), Literal("ghost"), Literal("Lb"), Literal("Bundle"), Literal("medium"), Literal("extra large")),
    None,
    List()
  )))


  private def q42 = Map(("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CATEGORY_ID", "I_CATEGORY", "I_MANAGER_ID"),
    Some("(I_MANAGER_ID IS NOT NULL AND (I_MANAGER_ID = ?))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q43 = Map(("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_DAY_NAME"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_ID", "S_STORE_NAME", "S_GMT_OFFSET"),
    Some("(S_GMT_OFFSET IS NOT NULL AND (S_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )))


  private def q44 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_NET_PROFIT"),
    Some("(SS_STORE_SK IS NOT NULL AND (SS_STORE_SK = ?))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_NET_PROFIT"),
    Some("(SS_STORE_SK IS NOT NULL AND (SS_STORE_SK = ?))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_PRODUCT_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_PRODUCT_NAME"),
    None,
    List(),
    None,
    List()
  )))


  private def q45 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_CITY", "CA_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    Some("(((D_QOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_QOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    Some("I_ITEM_SK IN ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"),
    List(Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(3.000000000000000000, 38, 18)), Literal(Decimal(5.000000000000000000, 38, 18)), Literal(Decimal(7.000000000000000000, 38, 18)), Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(13.000000000000000000, 38, 18)), Literal(Decimal(17.000000000000000000, 38, 18)), Literal(Decimal(19.000000000000000000, 38, 18)), Literal(Decimal(23.000000000000000000, 38, 18)), Literal(Decimal(29.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q46 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_COUPON_AMT", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("(((SS_STORE_SK IS NOT NULL AND SS_HDEMO_SK IS NOT NULL) AND SS_ADDR_SK IS NOT NULL) AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_DOW"),
    Some("(D_DOW IN ( ?, ? ) AND D_YEAR IN ( ?, ?, ? ))"),
    List(Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_CITY"),
    Some("S_CITY IN ( ?, ? )"),
    List(Literal("Fairview"), Literal("Midway")),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((HD_DEP_COUNT = ?) OR (HD_VEHICLE_COUNT = ?))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(3.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_CITY"),
    Some("CA_CITY IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_CITY"),
    Some("CA_CITY IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q47 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND I_BRAND IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR = ?) OR ((D_YEAR = ?) AND (D_MOY = ?))) OR ((D_YEAR = ?) AND (D_MOY = ?)))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_COMPANY_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND S_COMPANY_NAME IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND I_BRAND IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR = ?) OR ((D_YEAR = ?) AND (D_MOY = ?))) OR ((D_YEAR = ?) AND (D_MOY = ?)))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_COMPANY_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND S_COMPANY_NAME IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND I_BRAND IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR = ?) OR ((D_YEAR = ?) AND (D_MOY = ?))) OR ((D_YEAR = ?) AND (D_MOY = ?)))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_COMPANY_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND S_COMPANY_NAME IS NOT NULL)"),
    List(),
    None,
    List()
  )))


  private def q48 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_QUANTITY", "SS_SALES_PRICE", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("((SS_STORE_SK IS NOT NULL AND SS_CDEMO_SK IS NOT NULL) AND SS_ADDR_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS"),
    Some("((((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?)) OR ((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?))) OR ((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?)))"),
    List(Literal("M"), Literal("4 yr Degree"), Literal("D"), Literal("2 yr Degree"), Literal("S"), Literal("College")),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE", "CA_COUNTRY"),
    Some("((CA_COUNTRY IS NOT NULL AND (CA_COUNTRY = ?)) AND ((CA_STATE IN ( ?, ?, ? ) OR CA_STATE IN ( ?, ?, ? )) OR CA_STATE IN ( ?, ?, ? )))"),
    List(Literal("United States"), Literal("CO"), Literal("OH"), Literal("TX"), Literal("OR"), Literal("MN"), Literal("KY"), Literal("VA"), Literal("CA"), Literal("MS")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q49 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_NET_PAID", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    Some("(((((WS_NET_PROFIT IS NOT NULL AND WS_NET_PAID IS NOT NULL) AND WS_QUANTITY IS NOT NULL) AND (WS_NET_PROFIT > ?)) AND (WS_NET_PAID > ?)) AND (WS_QUANTITY > ?))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(0E-18, 38, 18))),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ITEM_SK", "WR_ORDER_NUMBER", "WR_RETURN_QUANTITY", "WR_RETURN_AMT"),
    Some("(WR_RETURN_AMT IS NOT NULL AND (WR_RETURN_AMT > ?))"),
    List(Literal(Decimal(10000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_NET_PAID", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    Some("(((((CS_NET_PROFIT IS NOT NULL AND CS_NET_PAID IS NOT NULL) AND CS_QUANTITY IS NOT NULL) AND (CS_NET_PROFIT > ?)) AND (CS_NET_PAID > ?)) AND (CS_QUANTITY > ?))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(0E-18, 38, 18))),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER", "CR_RETURN_QUANTITY", "CR_RETURN_AMOUNT"),
    Some("(CR_RETURN_AMOUNT IS NOT NULL AND (CR_RETURN_AMOUNT > ?))"),
    List(Literal(Decimal(10000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_NET_PAID", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("(((((SS_NET_PROFIT IS NOT NULL AND SS_NET_PAID IS NOT NULL) AND SS_QUANTITY IS NOT NULL) AND (SS_NET_PROFIT > ?)) AND (SS_NET_PAID > ?)) AND (SS_QUANTITY > ?))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(0E-18, 38, 18))),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER", "SR_RETURN_QUANTITY", "SR_RETURN_AMT"),
    Some("(SR_RETURN_AMT IS NOT NULL AND (SR_RETURN_AMT > ?))"),
    List(Literal(Decimal(10000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q50 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_SOLD_DATE_SK"),
    Some("(SS_CUSTOMER_SK IS NOT NULL AND SS_STORE_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_CUSTOMER_SK", "SR_TICKET_NUMBER", "SR_RETURNED_DATE_SK"),
    Some("SR_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_COMPANY_ID", "S_STREET_NUMBER", "S_STREET_NAME", "S_STREET_TYPE", "S_SUITE_NUMBER", "S_CITY", "S_COUNTY", "S_STATE", "S_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(8.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q51 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SALES_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q52 = Map(("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_BRAND", "I_MANAGER_ID"),
    Some("(I_MANAGER_ID IS NOT NULL AND (I_MANAGER_ID = ?))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q53 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CLASS", "I_CATEGORY", "I_MANUFACT_ID"),
    Some("(((I_CATEGORY IN ( ?, ?, ? ) AND I_CLASS IN ( ?, ?, ?, ? )) AND I_BRAND IN ( ?, ?, ?, ? )) OR ((I_CATEGORY IN ( ?, ?, ? ) AND I_CLASS IN ( ?, ?, ?, ? )) AND I_BRAND IN ( ?, ?, ?, ? )))"),
    List(Literal("Books"), Literal("Children"), Literal("Electronics"), Literal("personal"), Literal("portable"), Literal("reference"), Literal("self-help"), Literal("scholaramalgamalg #14"), Literal("scholaramalgamalg #7"), Literal("exportiunivamalg #9"), Literal("scholaramalgamalg #9"), Literal("Women"), Literal("Music"), Literal("Men"), Literal("accessories"), Literal("classical"), Literal("fragrances"), Literal("pants"), Literal("amalgimporto #1"), Literal("edu packscholar #1"), Literal("exportiimporto #1"), Literal("importoamalg #1")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ", "D_QOY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK"),
    None,
    List(),
    None,
    List()
  )))


  private def q54 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CLASS", "I_CATEGORY"),
    Some("(((I_CATEGORY IS NOT NULL AND I_CLASS IS NOT NULL) AND (I_CATEGORY = ?)) AND (I_CLASS = ?))"),
    List(Literal("Women"), Literal("maternity")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_COUNTY", "CA_STATE"),
    Some("(CA_COUNTY IS NOT NULL AND CA_STATE IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_COUNTY", "S_STATE"),
    Some("(S_COUNTY IS NOT NULL AND S_STATE IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("D_MONTH_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q55 = Map(("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_BRAND", "I_MANAGER_ID"),
    Some("(I_MANAGER_ID IS NOT NULL AND (I_MANAGER_ID = ?))"),
    List(Literal(Decimal(28.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q56 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_ADDR_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_ADDR_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_ID", "I_COLOR"),
    Some("I_COLOR IN ( ?, ?, ? )"),
    List(Literal("slate"), Literal("blanched"), Literal("burnished")),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_ADDR_SK", "CS_ITEM_SK", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_ID", "I_COLOR"),
    Some("I_COLOR IN ( ?, ?, ? )"),
    List(Literal("slate"), Literal("blanched"), Literal("burnished")),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_BILL_ADDR_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_ID", "I_COLOR"),
    Some("I_COLOR IN ( ?, ?, ? )"),
    List(Literal("slate"), Literal("blanched"), Literal("burnished")),
    None,
    List()
  )))


  private def q57 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND I_BRAND IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_CALL_CENTER_SK", "CS_ITEM_SK", "CS_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_CALL_CENTER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR = ?) OR ((D_YEAR = ?) AND (D_MOY = ?))) OR ((D_YEAR = ?) AND (D_MOY = ?)))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CALL_CENTER" -> ScanDetails(
    List("CC_CALL_CENTER_SK", "CC_NAME"),
    Some("CC_NAME IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND I_BRAND IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_CALL_CENTER_SK", "CS_ITEM_SK", "CS_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_CALL_CENTER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR = ?) OR ((D_YEAR = ?) AND (D_MOY = ?))) OR ((D_YEAR = ?) AND (D_MOY = ?)))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CALL_CENTER" -> ScanDetails(
    List("CC_CALL_CENTER_SK", "CC_NAME"),
    Some("CC_NAME IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND I_BRAND IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_CALL_CENTER_SK", "CS_ITEM_SK", "CS_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_CALL_CENTER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR = ?) OR ((D_YEAR = ?) AND (D_MOY = ?))) OR ((D_YEAR = ?) AND (D_MOY = ?)))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CALL_CENTER" -> ScanDetails(
    List("CC_CALL_CENTER_SK", "CC_NAME"),
    Some("CC_NAME IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q58 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q59 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_WEEK_SEQ", "D_DAY_NAME"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_ID", "S_STORE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_MONTH_SEQ", "D_WEEK_SEQ"),
    Some("(((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?)) AND D_WEEK_SEQ IS NOT NULL)"),
    List(Literal(Decimal(1212.000000000000000000, 38, 18)), Literal(Decimal(1223.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_WEEK_SEQ", "D_DAY_NAME"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_MONTH_SEQ", "D_WEEK_SEQ"),
    Some("(((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?)) AND D_WEEK_SEQ IS NOT NULL)"),
    List(Literal(Decimal(1224.000000000000000000, 38, 18)), Literal(Decimal(1235.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q60 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_ADDR_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_ADDR_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(9.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_ID", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?))"),
    List(Literal("Music")),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_ADDR_SK", "CS_ITEM_SK", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(9.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_ID", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?))"),
    List(Literal("Music")),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_BILL_ADDR_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_ADDR_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(9.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_ID", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?))"),
    List(Literal("Music")),
    None,
    List()
  )))


  private def q61 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_PROMO_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("((SS_STORE_SK IS NOT NULL AND SS_PROMO_SK IS NOT NULL) AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_GMT_OFFSET"),
    Some("(S_GMT_OFFSET IS NOT NULL AND (S_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK", "P_CHANNEL_DMAIL", "P_CHANNEL_EMAIL", "P_CHANNEL_TV"),
    Some("(((P_CHANNEL_DMAIL = ?) OR (P_CHANNEL_EMAIL = ?)) OR (P_CHANNEL_TV = ?))"),
    List(Literal("Y"), Literal("Y"), Literal("Y")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(11.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?))"),
    List(Literal("Jewelry")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_STORE_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("(SS_STORE_SK IS NOT NULL AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_GMT_OFFSET"),
    Some("(S_GMT_OFFSET IS NOT NULL AND (S_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(11.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-5.00, 5, 2))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CATEGORY"),
    Some("(I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?))"),
    List(Literal("Jewelry")),
    None,
    List()
  )))


  private def q62 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SHIP_DATE_SK", "WS_WEB_SITE_SK", "WS_SHIP_MODE_SK", "WS_WAREHOUSE_SK", "WS_SOLD_DATE_SK"),
    Some("(((WS_WAREHOUSE_SK IS NOT NULL AND WS_SHIP_MODE_SK IS NOT NULL) AND WS_WEB_SITE_SK IS NOT NULL) AND WS_SHIP_DATE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.SHIP_MODE" -> ScanDetails(
    List("SM_SHIP_MODE_SK", "SM_TYPE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SITE" -> ScanDetails(
    List("WEB_SITE_SK", "WEB_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q63 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CLASS", "I_CATEGORY", "I_MANAGER_ID"),
    Some("(((I_CATEGORY IN ( ?, ?, ? ) AND I_CLASS IN ( ?, ?, ?, ? )) AND I_BRAND IN ( ?, ?, ?, ? )) OR ((I_CATEGORY IN ( ?, ?, ? ) AND I_CLASS IN ( ?, ?, ?, ? )) AND I_BRAND IN ( ?, ?, ?, ? )))"),
    List(Literal("Books"), Literal("Children"), Literal("Electronics"), Literal("personal"), Literal("portable"), Literal("reference"), Literal("self-help"), Literal("scholaramalgamalg #14"), Literal("scholaramalgamalg #7"), Literal("exportiunivamalg #9"), Literal("scholaramalgamalg #9"), Literal("Women"), Literal("Music"), Literal("Men"), Literal("accessories"), Literal("classical"), Literal("fragrances"), Literal("pants"), Literal("amalgimporto #1"), Literal("edu packscholar #1"), Literal("exportiimporto #1"), Literal("importoamalg #1")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ", "D_MOY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK"),
    None,
    List(),
    None,
    List()
  )))


  private def q64 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_CDEMO_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_PROMO_SK", "SS_TICKET_NUMBER", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_COUPON_AMT", "SS_SOLD_DATE_SK"),
    Some("(((((SS_STORE_SK IS NOT NULL AND SS_CUSTOMER_SK IS NOT NULL) AND SS_CDEMO_SK IS NOT NULL) AND SS_PROMO_SK IS NOT NULL) AND SS_HDEMO_SK IS NOT NULL) AND SS_ADDR_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_ORDER_NUMBER", "CS_EXT_LIST_PRICE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER", "CR_REFUNDED_CASH", "CR_REVERSED_CHARGE", "CR_STORE_CREDIT"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_ZIP"),
    Some("(S_STORE_NAME IS NOT NULL AND S_ZIP IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_CDEMO_SK", "C_CURRENT_HDEMO_SK", "C_CURRENT_ADDR_SK", "C_FIRST_SHIPTO_DATE_SK", "C_FIRST_SALES_DATE_SK"),
    Some("((((C_FIRST_SALES_DATE_SK IS NOT NULL AND C_FIRST_SHIPTO_DATE_SK IS NOT NULL) AND C_CURRENT_CDEMO_SK IS NOT NULL) AND C_CURRENT_HDEMO_SK IS NOT NULL) AND C_CURRENT_ADDR_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS"),
    Some("CD_MARITAL_STATUS IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS"),
    Some("CD_MARITAL_STATUS IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_INCOME_BAND_SK"),
    Some("HD_INCOME_BAND_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_INCOME_BAND_SK"),
    Some("HD_INCOME_BAND_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STREET_NUMBER", "CA_STREET_NAME", "CA_CITY", "CA_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STREET_NUMBER", "CA_STREET_NAME", "CA_CITY", "CA_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.INCOME_BAND" -> ScanDetails(
    List("IB_INCOME_BAND_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.INCOME_BAND" -> ScanDetails(
    List("IB_INCOME_BAND_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CURRENT_PRICE", "I_COLOR", "I_PRODUCT_NAME"),
    Some("(((((I_CURRENT_PRICE IS NOT NULL AND I_COLOR IN ( ?, ?, ?, ?, ?, ? )) AND (I_CURRENT_PRICE >= ?)) AND (I_CURRENT_PRICE <= ?)) AND (I_CURRENT_PRICE >= ?)) AND (I_CURRENT_PRICE <= ?))"),
    List(Literal("purple"), Literal("burlywood"), Literal("indian"), Literal("spring"), Literal("floral"), Literal("medium"), Literal(Decimal(64.000000000000000000, 38, 18)), Literal(Decimal(74.000000000000000000, 38, 18)), Literal(Decimal(65.000000000000000000, 38, 18)), Literal(Decimal(79.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_CDEMO_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_PROMO_SK", "SS_TICKET_NUMBER", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_COUPON_AMT", "SS_SOLD_DATE_SK"),
    Some("(((((SS_STORE_SK IS NOT NULL AND SS_CUSTOMER_SK IS NOT NULL) AND SS_CDEMO_SK IS NOT NULL) AND SS_PROMO_SK IS NOT NULL) AND SS_HDEMO_SK IS NOT NULL) AND SS_ADDR_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_ORDER_NUMBER", "CS_EXT_LIST_PRICE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER", "CR_REFUNDED_CASH", "CR_REVERSED_CHARGE", "CR_STORE_CREDIT"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_ZIP"),
    Some("(S_STORE_NAME IS NOT NULL AND S_ZIP IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_CDEMO_SK", "C_CURRENT_HDEMO_SK", "C_CURRENT_ADDR_SK", "C_FIRST_SHIPTO_DATE_SK", "C_FIRST_SALES_DATE_SK"),
    Some("((((C_FIRST_SALES_DATE_SK IS NOT NULL AND C_FIRST_SHIPTO_DATE_SK IS NOT NULL) AND C_CURRENT_CDEMO_SK IS NOT NULL) AND C_CURRENT_HDEMO_SK IS NOT NULL) AND C_CURRENT_ADDR_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS"),
    Some("CD_MARITAL_STATUS IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS"),
    Some("CD_MARITAL_STATUS IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_INCOME_BAND_SK"),
    Some("HD_INCOME_BAND_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_INCOME_BAND_SK"),
    Some("HD_INCOME_BAND_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STREET_NUMBER", "CA_STREET_NAME", "CA_CITY", "CA_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STREET_NUMBER", "CA_STREET_NAME", "CA_CITY", "CA_ZIP"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.INCOME_BAND" -> ScanDetails(
    List("IB_INCOME_BAND_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.INCOME_BAND" -> ScanDetails(
    List("IB_INCOME_BAND_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CURRENT_PRICE", "I_COLOR", "I_PRODUCT_NAME"),
    Some("(((((I_CURRENT_PRICE IS NOT NULL AND I_COLOR IN ( ?, ?, ?, ?, ?, ? )) AND (I_CURRENT_PRICE >= ?)) AND (I_CURRENT_PRICE <= ?)) AND (I_CURRENT_PRICE >= ?)) AND (I_CURRENT_PRICE <= ?))"),
    List(Literal("purple"), Literal("burlywood"), Literal("indian"), Literal("spring"), Literal("floral"), Literal("medium"), Literal(Decimal(64.000000000000000000, 38, 18)), Literal(Decimal(74.000000000000000000, 38, 18)), Literal(Decimal(65.000000000000000000, 38, 18)), Literal(Decimal(79.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q65 = Map(("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1176.000000000000000000, 38, 18)), Literal(Decimal(1187.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_DESC", "I_CURRENT_PRICE", "I_WHOLESALE_COST", "I_BRAND"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1176.000000000000000000, 38, 18)), Literal(Decimal(1187.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q66 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SOLD_TIME_SK", "WS_SHIP_MODE_SK", "WS_WAREHOUSE_SK", "WS_QUANTITY", "WS_EXT_SALES_PRICE", "WS_NET_PAID", "WS_SOLD_DATE_SK"),
    Some("((WS_WAREHOUSE_SK IS NOT NULL AND WS_SOLD_TIME_SK IS NOT NULL) AND WS_SHIP_MODE_SK IS NOT NULL)"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME", "W_WAREHOUSE_SQ_FT", "W_CITY", "W_COUNTY", "W_STATE", "W_COUNTRY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_TIME"),
    Some("((T_TIME IS NOT NULL AND (T_TIME >= ?)) AND (T_TIME <= ?))"),
    List(Literal(Decimal(30838.000000000000000000, 38, 18)), Literal(Decimal(59638.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.SHIP_MODE" -> ScanDetails(
    List("SM_SHIP_MODE_SK", "SM_CARRIER"),
    Some("SM_CARRIER IN ( ?, ? )"),
    List(Literal("DHL"), Literal("BARIAN")),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SOLD_TIME_SK", "CS_SHIP_MODE_SK", "CS_WAREHOUSE_SK", "CS_QUANTITY", "CS_SALES_PRICE", "CS_NET_PAID_INC_TAX", "CS_SOLD_DATE_SK"),
    Some("((CS_WAREHOUSE_SK IS NOT NULL AND CS_SOLD_TIME_SK IS NOT NULL) AND CS_SHIP_MODE_SK IS NOT NULL)"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME", "W_WAREHOUSE_SQ_FT", "W_CITY", "W_COUNTY", "W_STATE", "W_COUNTRY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_TIME"),
    Some("((T_TIME IS NOT NULL AND (T_TIME >= ?)) AND (T_TIME <= ?))"),
    List(Literal(Decimal(30838.000000000000000000, 38, 18)), Literal(Decimal(59638.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.SHIP_MODE" -> ScanDetails(
    List("SM_SHIP_MODE_SK", "SM_CARRIER"),
    Some("SM_CARRIER IN ( ?, ? )"),
    List(Literal("DHL"), Literal("BARIAN")),
    None,
    List()
  )))


  private def q67 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_QUANTITY", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ", "D_YEAR", "D_MOY", "D_QOY"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CLASS", "I_CATEGORY", "I_PRODUCT_NAME"),
    None,
    List(),
    None,
    List()
  )))


  private def q68 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_EXT_SALES_PRICE", "SS_EXT_LIST_PRICE", "SS_EXT_TAX", "SS_SOLD_DATE_SK"),
    Some("(((SS_STORE_SK IS NOT NULL AND SS_HDEMO_SK IS NOT NULL) AND SS_ADDR_SK IS NOT NULL) AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_DOM"),
    Some("(((D_DOM IS NOT NULL AND (D_DOM >= ?)) AND (D_DOM <= ?)) AND D_YEAR IN ( ?, ?, ? ))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_CITY"),
    Some("S_CITY IN ( ?, ? )"),
    List(Literal("Fairview"), Literal("Midway")),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((HD_DEP_COUNT = ?) OR (HD_VEHICLE_COUNT = ?))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(3.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_CITY"),
    Some("CA_CITY IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_ADDR_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_CITY"),
    Some("CA_CITY IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q69 = Map(("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_CDEMO_SK", "C_CURRENT_ADDR_SK"),
    Some("(C_CURRENT_ADDR_SK IS NOT NULL AND C_CURRENT_CDEMO_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_CDEMO_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_PROMO_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE", "SS_SALES_PRICE", "SS_EXT_DISCOUNT_AMT", "SS_EXT_SALES_PRICE", "SS_EXT_WHOLESALE_COST", "SS_EXT_LIST_PRICE", "SS_EXT_TAX", "SS_COUPON_AMT", "SS_NET_PAID", "SS_NET_PAID_INC_TAX", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("((((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY >= ?)) AND (D_MOY <= ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SOLD_TIME_SK", "WS_SHIP_DATE_SK", "WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_BILL_CDEMO_SK", "WS_BILL_HDEMO_SK", "WS_BILL_ADDR_SK", "WS_SHIP_CUSTOMER_SK", "WS_SHIP_CDEMO_SK", "WS_SHIP_HDEMO_SK", "WS_SHIP_ADDR_SK", "WS_WEB_PAGE_SK", "WS_WEB_SITE_SK", "WS_SHIP_MODE_SK", "WS_WAREHOUSE_SK", "WS_PROMO_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_WHOLESALE_COST", "WS_LIST_PRICE", "WS_SALES_PRICE", "WS_EXT_DISCOUNT_AMT", "WS_EXT_SALES_PRICE", "WS_EXT_WHOLESALE_COST", "WS_EXT_LIST_PRICE", "WS_EXT_TAX", "WS_COUPON_AMT", "WS_EXT_SHIP_COST", "WS_NET_PAID", "WS_NET_PAID_INC_TAX", "WS_NET_PAID_INC_SHIP", "WS_NET_PAID_INC_SHIP_TAX", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("((((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY >= ?)) AND (D_MOY <= ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SOLD_TIME_SK", "CS_SHIP_DATE_SK", "CS_BILL_CUSTOMER_SK", "CS_BILL_CDEMO_SK", "CS_BILL_HDEMO_SK", "CS_BILL_ADDR_SK", "CS_SHIP_CUSTOMER_SK", "CS_SHIP_CDEMO_SK", "CS_SHIP_HDEMO_SK", "CS_SHIP_ADDR_SK", "CS_CALL_CENTER_SK", "CS_CATALOG_PAGE_SK", "CS_SHIP_MODE_SK", "CS_WAREHOUSE_SK", "CS_ITEM_SK", "CS_PROMO_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_WHOLESALE_COST", "CS_LIST_PRICE", "CS_SALES_PRICE", "CS_EXT_DISCOUNT_AMT", "CS_EXT_SALES_PRICE", "CS_EXT_WHOLESALE_COST", "CS_EXT_LIST_PRICE", "CS_EXT_TAX", "CS_COUPON_AMT", "CS_EXT_SHIP_COST", "CS_NET_PAID", "CS_NET_PAID_INC_TAX", "CS_NET_PAID_INC_SHIP", "CS_NET_PAID_INC_SHIP_TAX", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE_ID", "D_DATE", "D_MONTH_SEQ", "D_WEEK_SEQ", "D_QUARTER_SEQ", "D_YEAR", "D_DOW", "D_MOY", "D_DOM", "D_QOY", "D_FY_YEAR", "D_FY_QUARTER_SEQ", "D_FY_WEEK_SEQ", "D_DAY_NAME", "D_QUARTER_NAME", "D_HOLIDAY", "D_WEEKEND", "D_FOLLOWING_HOLIDAY", "D_FIRST_DOM", "D_LAST_DOM", "D_SAME_DAY_LY", "D_SAME_DAY_LQ", "D_CURRENT_DAY", "D_CURRENT_WEEK", "D_CURRENT_MONTH", "D_CURRENT_QUARTER", "D_CURRENT_YEAR"),
    Some("((((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY >= ?)) AND (D_MOY <= ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("CA_STATE IN ( ?, ?, ? )"),
    List(Literal("KY"), Literal("GA"), Literal("NM")),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_GENDER", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS", "CD_PURCHASE_ESTIMATE", "CD_CREDIT_RATING"),
    None,
    List(),
    None,
    List()
  )))


  private def q70 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_STORE_SK", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_COUNTY", "S_STATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_STORE_SK", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q71 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_BRAND", "I_MANAGER_ID"),
    Some("(I_MANAGER_ID IS NOT NULL AND (I_MANAGER_ID = ?))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SOLD_TIME_SK", "WS_ITEM_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_SOLD_TIME_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SOLD_TIME_SK", "CS_ITEM_SK", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_SOLD_TIME_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_ITEM_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_SOLD_TIME_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_MOY IS NOT NULL AND D_YEAR IS NOT NULL) AND (D_MOY = ?)) AND (D_YEAR = ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE", "T_MEAL_TIME"),
    Some("((T_MEAL_TIME = ?) OR (T_MEAL_TIME = ?))"),
    List(Literal("breakfast"), Literal("dinner")),
    None,
    List()
  )))


  private def q72 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SHIP_DATE_SK", "CS_BILL_CDEMO_SK", "CS_BILL_HDEMO_SK", "CS_ITEM_SK", "CS_PROMO_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_SOLD_DATE_SK"),
    Some("(((CS_QUANTITY IS NOT NULL AND CS_BILL_CDEMO_SK IS NOT NULL) AND CS_BILL_HDEMO_SK IS NOT NULL) AND CS_SHIP_DATE_SK IS NOT NULL)"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_WAREHOUSE_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    Some("INV_QUANTITY_ON_HAND IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_DESC"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS"),
    Some("(CD_MARITAL_STATUS IS NOT NULL AND (CD_MARITAL_STATUS = ?))"),
    List(Literal("D")),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_BUY_POTENTIAL"),
    Some("(HD_BUY_POTENTIAL IS NOT NULL AND (HD_BUY_POTENTIAL = ?))"),
    List(Literal(">10000")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_WEEK_SEQ", "D_YEAR"),
    Some("(((D_YEAR IS NOT NULL AND (D_YEAR = ?)) AND D_WEEK_SEQ IS NOT NULL) AND D_DATE IS NOT NULL)"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_WEEK_SEQ"),
    Some("D_WEEK_SEQ IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("D_DATE IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER"),
    None,
    List(),
    None,
    List()
  )))


  private def q73 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_HDEMO_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_SOLD_DATE_SK"),
    Some("((SS_STORE_SK IS NOT NULL AND SS_HDEMO_SK IS NOT NULL) AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_DOM"),
    Some("(((D_DOM IS NOT NULL AND (D_DOM >= ?)) AND (D_DOM <= ?)) AND D_YEAR IN ( ?, ?, ? ))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_COUNTY"),
    Some("S_COUNTY IN ( ?, ?, ?, ? )"),
    List(Literal("Williamson County"), Literal("Franklin Parish"), Literal("Bronx County"), Literal("Orange County")),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_BUY_POTENTIAL", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((HD_VEHICLE_COUNT IS NOT NULL AND ((HD_BUY_POTENTIAL = ?) OR (HD_BUY_POTENTIAL = ?))) AND (HD_VEHICLE_COUNT > ?))"),
    List(Literal(">10000"), Literal("Unknown"), Literal(Decimal(0E-18, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_SALUTATION", "C_FIRST_NAME", "C_LAST_NAME", "C_PREFERRED_CUST_FLAG"),
    None,
    List(),
    None,
    List()
  )))


  private def q74 = Map(("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_NET_PAID", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR = ?)) AND D_YEAR IN ( ?, ? ))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_NET_PAID", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR = ?)) AND D_YEAR IN ( ?, ? ))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_CUSTOMER_SK", "WS_NET_PAID", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR = ?)) AND D_YEAR IN ( ?, ? ))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_CUSTOMER_SK", "WS_NET_PAID", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR IS NOT NULL AND (D_YEAR = ?)) AND D_YEAR IN ( ?, ? ))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18)), Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q75 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID", "I_CATEGORY", "I_MANUFACT_ID"),
    Some("(((((I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?)) AND I_BRAND_ID IS NOT NULL) AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL) AND I_MANUFACT_ID IS NOT NULL)"),
    List(Literal("Books")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER", "CR_RETURN_QUANTITY", "CR_RETURN_AMOUNT"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID", "I_CATEGORY", "I_MANUFACT_ID"),
    Some("(((((I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?)) AND I_BRAND_ID IS NOT NULL) AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL) AND I_MANUFACT_ID IS NOT NULL)"),
    List(Literal("Books")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER", "SR_RETURN_QUANTITY", "SR_RETURN_AMT"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID", "I_CATEGORY", "I_MANUFACT_ID"),
    Some("(((((I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?)) AND I_BRAND_ID IS NOT NULL) AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL) AND I_MANUFACT_ID IS NOT NULL)"),
    List(Literal("Books")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2002.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ITEM_SK", "WR_ORDER_NUMBER", "WR_RETURN_QUANTITY", "WR_RETURN_AMT"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_ITEM_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID", "I_CATEGORY", "I_MANUFACT_ID"),
    Some("(((((I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?)) AND I_BRAND_ID IS NOT NULL) AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL) AND I_MANUFACT_ID IS NOT NULL)"),
    List(Literal("Books")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER", "CR_RETURN_QUANTITY", "CR_RETURN_AMOUNT"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID", "I_CATEGORY", "I_MANUFACT_ID"),
    Some("(((((I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?)) AND I_BRAND_ID IS NOT NULL) AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL) AND I_MANUFACT_ID IS NOT NULL)"),
    List(Literal("Books")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER", "SR_RETURN_QUANTITY", "SR_RETURN_AMT"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND_ID", "I_CLASS_ID", "I_CATEGORY_ID", "I_CATEGORY", "I_MANUFACT_ID"),
    Some("(((((I_CATEGORY IS NOT NULL AND (I_CATEGORY = ?)) AND I_BRAND_ID IS NOT NULL) AND I_CLASS_ID IS NOT NULL) AND I_CATEGORY_ID IS NOT NULL) AND I_MANUFACT_ID IS NOT NULL)"),
    List(Literal("Books")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ITEM_SK", "WR_ORDER_NUMBER", "WR_RETURN_QUANTITY", "WR_RETURN_AMT"),
    None,
    List(),
    None,
    List()
  )))


  private def q76 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CATEGORY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_SHIP_CUSTOMER_SK", "WS_EXT_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_SHIP_CUSTOMER_SK IS NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CATEGORY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SHIP_ADDR_SK", "CS_ITEM_SK", "CS_EXT_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_SHIP_ADDR_SK IS NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CATEGORY"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_QOY"),
    None,
    List(),
    None,
    List()
  )))


  private def q77 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_STORE_SK", "SS_EXT_SALES_PRICE", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_STORE_SK", "SR_RETURN_AMT", "SR_NET_LOSS", "SR_RETURNED_DATE_SK"),
    Some("SR_STORE_SK IS NOT NULL"),
    List(),
    Some("SR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_CALL_CENTER_SK", "CS_EXT_SALES_PRICE", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_CALL_CENTER_SK", "CR_RETURN_AMOUNT", "CR_NET_LOSS", "CR_RETURNED_DATE_SK"),
    None,
    List(),
    Some("CR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_WEB_PAGE_SK", "WS_EXT_SALES_PRICE", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    Some("WS_WEB_PAGE_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.WEB_PAGE" -> ScanDetails(
    List("WP_WEB_PAGE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_WEB_PAGE_SK", "WR_RETURN_AMT", "WR_NET_LOSS", "WR_RETURNED_DATE_SK"),
    Some("WR_WEB_PAGE_SK IS NOT NULL"),
    List(),
    Some("WR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.WEB_PAGE" -> ScanDetails(
    List("WP_WEB_PAGE_SK"),
    None,
    List(),
    None,
    List()
  )))


  private def q78 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_WHOLESALE_COST", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_WHOLESALE_COST", "WS_SALES_PRICE", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ITEM_SK", "WR_ORDER_NUMBER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR = ?) AND D_YEAR IS NOT NULL)"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_ITEM_SK", "CS_ORDER_NUMBER", "CS_QUANTITY", "CS_WHOLESALE_COST", "CS_SALES_PRICE", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("((D_YEAR = ?) AND D_YEAR IS NOT NULL)"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q79 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_HDEMO_SK", "SS_ADDR_SK", "SS_STORE_SK", "SS_TICKET_NUMBER", "SS_COUPON_AMT", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("((SS_STORE_SK IS NOT NULL AND SS_HDEMO_SK IS NOT NULL) AND SS_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_DOW"),
    Some("((D_DOW IS NOT NULL AND (D_DOW = ?)) AND D_YEAR IN ( ?, ?, ? ))"),
    List(Literal(Decimal(1.000000000000000000, 38, 18)), Literal(Decimal(1999.000000000000000000, 38, 18)), Literal(Decimal(2000.000000000000000000, 38, 18)), Literal(Decimal(2001.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_NUMBER_EMPLOYEES", "S_CITY"),
    Some("((S_NUMBER_EMPLOYEES IS NOT NULL AND (S_NUMBER_EMPLOYEES >= ?)) AND (S_NUMBER_EMPLOYEES <= ?))"),
    List(Literal(Decimal(200.000000000000000000, 38, 18)), Literal(Decimal(295.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((HD_DEP_COUNT = ?) OR (HD_VEHICLE_COUNT > ?))"),
    List(Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )))


  private def q80 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_PROMO_SK", "SS_TICKET_NUMBER", "SS_EXT_SALES_PRICE", "SS_NET_PROFIT", "SS_SOLD_DATE_SK"),
    Some("(SS_STORE_SK IS NOT NULL AND SS_PROMO_SK IS NOT NULL)"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_TICKET_NUMBER", "SR_RETURN_AMT", "SR_NET_LOSS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CURRENT_PRICE"),
    Some("(I_CURRENT_PRICE IS NOT NULL AND (I_CURRENT_PRICE > ?))"),
    List(Literal(Decimal(50.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK", "P_CHANNEL_TV"),
    Some("(P_CHANNEL_TV IS NOT NULL AND (P_CHANNEL_TV = ?))"),
    List(Literal("N")),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_CATALOG_PAGE_SK", "CS_ITEM_SK", "CS_PROMO_SK", "CS_ORDER_NUMBER", "CS_EXT_SALES_PRICE", "CS_NET_PROFIT", "CS_SOLD_DATE_SK"),
    Some("(CS_CATALOG_PAGE_SK IS NOT NULL AND CS_PROMO_SK IS NOT NULL)"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_ORDER_NUMBER", "CR_RETURN_AMOUNT", "CR_NET_LOSS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.CATALOG_PAGE" -> ScanDetails(
    List("CP_CATALOG_PAGE_SK", "CP_CATALOG_PAGE_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CURRENT_PRICE"),
    Some("(I_CURRENT_PRICE IS NOT NULL AND (I_CURRENT_PRICE > ?))"),
    List(Literal(Decimal(50.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK", "P_CHANNEL_TV"),
    Some("(P_CHANNEL_TV IS NOT NULL AND (P_CHANNEL_TV = ?))"),
    List(Literal("N")),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_WEB_SITE_SK", "WS_PROMO_SK", "WS_ORDER_NUMBER", "WS_EXT_SALES_PRICE", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    Some("(WS_WEB_SITE_SK IS NOT NULL AND WS_PROMO_SK IS NOT NULL)"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ITEM_SK", "WR_ORDER_NUMBER", "WR_RETURN_AMT", "WR_NET_LOSS"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-08-23")), Literal(Date.valueOf("2000-09-22"))),
    None,
    List()
  )),("TPCDS.WEB_SITE" -> ScanDetails(
    List("WEB_SITE_SK", "WEB_SITE_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CURRENT_PRICE"),
    Some("(I_CURRENT_PRICE IS NOT NULL AND (I_CURRENT_PRICE > ?))"),
    List(Literal(Decimal(50.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.PROMOTION" -> ScanDetails(
    List("P_PROMO_SK", "P_CHANNEL_TV"),
    Some("(P_CHANNEL_TV IS NOT NULL AND (P_CHANNEL_TV = ?))"),
    List(Literal("N")),
    None,
    List()
  )))


  private def q81 = Map(("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_RETURNING_CUSTOMER_SK", "CR_RETURNING_ADDR_SK", "CR_RETURN_AMT_INC_TAX", "CR_RETURNED_DATE_SK"),
    Some("(CR_RETURNING_ADDR_SK IS NOT NULL AND CR_RETURNING_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("CR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("CA_STATE IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_RETURNING_CUSTOMER_SK", "CR_RETURNING_ADDR_SK", "CR_RETURN_AMT_INC_TAX", "CR_RETURNED_DATE_SK"),
    Some("CR_RETURNING_ADDR_SK IS NOT NULL"),
    List(),
    Some("CR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("CA_STATE IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CUSTOMER_ID", "C_CURRENT_ADDR_SK", "C_SALUTATION", "C_FIRST_NAME", "C_LAST_NAME"),
    Some("C_CURRENT_ADDR_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STREET_NUMBER", "CA_STREET_NAME", "CA_STREET_TYPE", "CA_SUITE_NUMBER", "CA_CITY", "CA_COUNTY", "CA_STATE", "CA_ZIP", "CA_COUNTRY", "CA_GMT_OFFSET", "CA_LOCATION_TYPE"),
    Some("(CA_STATE IS NOT NULL AND (CA_STATE = ?))"),
    List(Literal("GA")),
    None,
    List()
  )))


  private def q82 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_ITEM_DESC", "I_CURRENT_PRICE", "I_MANUFACT_ID"),
    Some("(((I_CURRENT_PRICE IS NOT NULL AND (I_CURRENT_PRICE >= ?)) AND (I_CURRENT_PRICE <= ?)) AND I_MANUFACT_ID IN ( ?, ?, ?, ? ))"),
    List(Literal(Decimal(62.000000000000000000, 38, 18)), Literal(Decimal(92.000000000000000000, 38, 18)), Literal(Decimal(129.000000000000000000, 38, 18)), Literal(Decimal(270.000000000000000000, 38, 18)), Literal(Decimal(821.000000000000000000, 38, 18)), Literal(Decimal(423.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.INVENTORY" -> ScanDetails(
    List("INV_ITEM_SK", "INV_QUANTITY_ON_HAND", "INV_DATE_SK"),
    Some("((INV_QUANTITY_ON_HAND IS NOT NULL AND (INV_QUANTITY_ON_HAND >= ?)) AND (INV_QUANTITY_ON_HAND <= ?))"),
    List(Literal(Decimal(100.000000000000000000, 38, 18)), Literal(Decimal(500.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-05-25")), Literal(Date.valueOf("2000-07-24"))),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK"),
    None,
    List(),
    None,
    List()
  )))


  private def q83 = Map(("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_RETURN_QUANTITY", "SR_RETURNED_DATE_SK"),
    None,
    List(),
    Some("SR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_ITEM_SK", "CR_RETURN_QUANTITY", "CR_RETURNED_DATE_SK"),
    None,
    List(),
    Some("CR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ITEM_SK", "WR_RETURN_QUANTITY", "WR_RETURNED_DATE_SK"),
    None,
    List(),
    Some("WR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE", "D_WEEK_SEQ"),
    None,
    List(),
    None,
    List()
  )))


  private def q84 = Map(("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_ID", "C_CURRENT_CDEMO_SK", "C_CURRENT_HDEMO_SK", "C_CURRENT_ADDR_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    Some("((C_CURRENT_ADDR_SK IS NOT NULL AND C_CURRENT_CDEMO_SK IS NOT NULL) AND C_CURRENT_HDEMO_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_CITY"),
    Some("(CA_CITY IS NOT NULL AND (CA_CITY = ?))"),
    List(Literal("Edgewood")),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_INCOME_BAND_SK"),
    Some("HD_INCOME_BAND_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.INCOME_BAND" -> ScanDetails(
    List("IB_INCOME_BAND_SK", "IB_LOWER_BOUND", "IB_UPPER_BOUND"),
    Some("(((IB_LOWER_BOUND IS NOT NULL AND IB_UPPER_BOUND IS NOT NULL) AND (IB_LOWER_BOUND >= ?)) AND (IB_UPPER_BOUND <= ?))"),
    List(Literal(Decimal(38128.000000000000000000, 38, 18)), Literal(Decimal(88128.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_CDEMO_SK"),
    Some("SR_CDEMO_SK IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q85 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_WEB_PAGE_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_SALES_PRICE", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    Some("WS_WEB_PAGE_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ITEM_SK", "WR_REFUNDED_CDEMO_SK", "WR_REFUNDED_ADDR_SK", "WR_RETURNING_CDEMO_SK", "WR_REASON_SK", "WR_ORDER_NUMBER", "WR_FEE", "WR_REFUNDED_CASH"),
    Some("(((WR_REFUNDED_CDEMO_SK IS NOT NULL AND WR_RETURNING_CDEMO_SK IS NOT NULL) AND WR_REFUNDED_ADDR_SK IS NOT NULL) AND WR_REASON_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_PAGE" -> ScanDetails(
    List("WP_WEB_PAGE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS"),
    Some("((CD_MARITAL_STATUS IS NOT NULL AND CD_EDUCATION_STATUS IS NOT NULL) AND ((((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?)) OR ((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?))) OR ((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?))))"),
    List(Literal("M"), Literal("Advanced Degree"), Literal("S"), Literal("College"), Literal("W"), Literal("2 yr Degree")),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS"),
    Some("(CD_MARITAL_STATUS IS NOT NULL AND CD_EDUCATION_STATUS IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE", "CA_COUNTRY"),
    Some("((CA_COUNTRY IS NOT NULL AND (CA_COUNTRY = ?)) AND ((CA_STATE IN ( ?, ?, ? ) OR CA_STATE IN ( ?, ?, ? )) OR CA_STATE IN ( ?, ?, ? )))"),
    List(Literal("United States"), Literal("IN"), Literal("OH"), Literal("NJ"), Literal("WI"), Literal("CT"), Literal("KY"), Literal("LA"), Literal("IA"), Literal("AR")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(2000.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.REASON" -> ScanDetails(
    List("R_REASON_SK", "R_REASON_DESC"),
    None,
    List(),
    None,
    List()
  )))


  private def q86 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_NET_PAID", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_CLASS", "I_CATEGORY"),
    None,
    List(),
    None,
    List()
  )))


  private def q87 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_CUSTOMER_SK", "SS_SOLD_DATE_SK"),
    Some("SS_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_SOLD_DATE_SK"),
    Some("CS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_BILL_CUSTOMER_SK", "WS_SOLD_DATE_SK"),
    Some("WS_BILL_CUSTOMER_SK IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME"),
    None,
    List(),
    None,
    List()
  )))


  private def q88 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?))) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE >= ?))"),
    List(Literal(Decimal(8.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?))) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE < ?))"),
    List(Literal(Decimal(9.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?))) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE >= ?))"),
    List(Literal(Decimal(9.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?))) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE < ?))"),
    List(Literal(Decimal(10.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?))) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE >= ?))"),
    List(Literal(Decimal(10.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?))) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE < ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?))) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE >= ?))"),
    List(Literal(Decimal(11.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT", "HD_VEHICLE_COUNT"),
    Some("((((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?))) OR ((HD_DEP_COUNT = ?) AND (HD_VEHICLE_COUNT <= ?)))"),
    List(Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(6.000000000000000000, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18)), Literal(Decimal(4.000000000000000000, 38, 18)), Literal(Decimal(0E-18, 38, 18)), Literal(Decimal(2.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE < ?))"),
    List(Literal(Decimal(12.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )))


  private def q89 = Map(("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_BRAND", "I_CLASS", "I_CATEGORY"),
    Some("((I_CATEGORY IN ( ?, ?, ? ) AND I_CLASS IN ( ?, ?, ? )) OR (I_CATEGORY IN ( ?, ?, ? ) AND I_CLASS IN ( ?, ?, ? )))"),
    List(Literal("Books"), Literal("Electronics"), Literal("Sports"), Literal("computers"), Literal("stereo"), Literal("football"), Literal("Men"), Literal("Jewelry"), Literal("Women"), Literal("shirts"), Literal("birdal"), Literal("dresses")),
    None,
    List()
  )),("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_STORE_SK", "SS_SALES_PRICE", "SS_SOLD_DATE_SK"),
    Some("SS_STORE_SK IS NOT NULL"),
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(D_YEAR IS NOT NULL AND (D_YEAR = ?))"),
    List(Literal(Decimal(1999.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME", "S_COMPANY_NAME"),
    None,
    List(),
    None,
    List()
  )))


  private def q90 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SOLD_TIME_SK", "WS_SHIP_HDEMO_SK", "WS_WEB_PAGE_SK"),
    Some("((WS_SHIP_HDEMO_SK IS NOT NULL AND WS_SOLD_TIME_SK IS NOT NULL) AND WS_WEB_PAGE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT"),
    Some("(HD_DEP_COUNT IS NOT NULL AND (HD_DEP_COUNT = ?))"),
    List(Literal(Decimal(6.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR"),
    Some("((T_HOUR IS NOT NULL AND (T_HOUR >= ?)) AND (T_HOUR <= ?))"),
    List(Literal(Decimal(8.000000000000000000, 38, 18)), Literal(Decimal(9.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_PAGE" -> ScanDetails(
    List("WP_WEB_PAGE_SK", "WP_CHAR_COUNT"),
    Some("((WP_CHAR_COUNT IS NOT NULL AND (WP_CHAR_COUNT >= ?)) AND (WP_CHAR_COUNT <= ?))"),
    List(Literal(Decimal(5000.000000000000000000, 38, 18)), Literal(Decimal(5200.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SOLD_TIME_SK", "WS_SHIP_HDEMO_SK", "WS_WEB_PAGE_SK"),
    Some("((WS_SHIP_HDEMO_SK IS NOT NULL AND WS_SOLD_TIME_SK IS NOT NULL) AND WS_WEB_PAGE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT"),
    Some("(HD_DEP_COUNT IS NOT NULL AND (HD_DEP_COUNT = ?))"),
    List(Literal(Decimal(6.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR"),
    Some("((T_HOUR IS NOT NULL AND (T_HOUR >= ?)) AND (T_HOUR <= ?))"),
    List(Literal(Decimal(19.000000000000000000, 38, 18)), Literal(Decimal(20.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_PAGE" -> ScanDetails(
    List("WP_WEB_PAGE_SK", "WP_CHAR_COUNT"),
    Some("((WP_CHAR_COUNT IS NOT NULL AND (WP_CHAR_COUNT >= ?)) AND (WP_CHAR_COUNT <= ?))"),
    List(Literal(Decimal(5000.000000000000000000, 38, 18)), Literal(Decimal(5200.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q91 = Map(("TPCDS.CALL_CENTER" -> ScanDetails(
    List("CC_CALL_CENTER_SK", "CC_CALL_CENTER_ID", "CC_NAME", "CC_MANAGER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CATALOG_RETURNS" -> ScanDetails(
    List("CR_RETURNING_CUSTOMER_SK", "CR_CALL_CENTER_SK", "CR_NET_LOSS", "CR_RETURNED_DATE_SK"),
    Some("(CR_CALL_CENTER_SK IS NOT NULL AND CR_RETURNING_CUSTOMER_SK IS NOT NULL)"),
    List(),
    Some("CR_RETURNED_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_YEAR", "D_MOY"),
    Some("(((D_YEAR IS NOT NULL AND D_MOY IS NOT NULL) AND (D_YEAR = ?)) AND (D_MOY = ?))"),
    List(Literal(Decimal(1998.000000000000000000, 38, 18)), Literal(Decimal(11.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CUSTOMER" -> ScanDetails(
    List("C_CUSTOMER_SK", "C_CURRENT_CDEMO_SK", "C_CURRENT_HDEMO_SK", "C_CURRENT_ADDR_SK"),
    Some("((C_CURRENT_ADDR_SK IS NOT NULL AND C_CURRENT_CDEMO_SK IS NOT NULL) AND C_CURRENT_HDEMO_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_GMT_OFFSET"),
    Some("(CA_GMT_OFFSET IS NOT NULL AND (CA_GMT_OFFSET = ?))"),
    List(Literal(Decimal(-7.00, 5, 2))),
    None,
    List()
  )),("TPCDS.CUSTOMER_DEMOGRAPHICS" -> ScanDetails(
    List("CD_DEMO_SK", "CD_MARITAL_STATUS", "CD_EDUCATION_STATUS"),
    Some("(((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?)) OR ((CD_MARITAL_STATUS = ?) AND (CD_EDUCATION_STATUS = ?)))"),
    List(Literal("M"), Literal("Unknown"), Literal("W"), Literal("Advanced Degree")),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_BUY_POTENTIAL"),
    Some("HD_BUY_POTENTIAL IS NOT NULL"),
    List(),
    None,
    List()
  )))


  private def q92 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_EXT_DISCOUNT_AMT", "WS_SOLD_DATE_SK"),
    Some("WS_EXT_DISCOUNT_AMT IS NOT NULL"),
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_MANUFACT_ID"),
    Some("(I_MANUFACT_ID IS NOT NULL AND (I_MANUFACT_ID = ?))"),
    List(Literal(Decimal(350.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_ITEM_SK", "WS_EXT_DISCOUNT_AMT", "WS_SOLD_DATE_SK"),
    None,
    List(),
    Some("WS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-01-27")), Literal(Date.valueOf("2000-04-26"))),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("2000-01-27")), Literal(Date.valueOf("2000-04-26"))),
    None,
    List()
  )))


  private def q93 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_TICKET_NUMBER", "SS_QUANTITY", "SS_SALES_PRICE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.STORE_RETURNS" -> ScanDetails(
    List("SR_ITEM_SK", "SR_REASON_SK", "SR_TICKET_NUMBER", "SR_RETURN_QUANTITY"),
    Some("SR_REASON_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.REASON" -> ScanDetails(
    List("R_REASON_SK", "R_REASON_DESC"),
    Some("(R_REASON_DESC IS NOT NULL AND (R_REASON_DESC = ?))"),
    List(Literal("reason 28")),
    None,
    List()
  )))


  private def q94 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SHIP_DATE_SK", "WS_SHIP_ADDR_SK", "WS_WEB_SITE_SK", "WS_WAREHOUSE_SK", "WS_ORDER_NUMBER", "WS_EXT_SHIP_COST", "WS_NET_PROFIT"),
    Some("((WS_SHIP_DATE_SK IS NOT NULL AND WS_SHIP_ADDR_SK IS NOT NULL) AND WS_WEB_SITE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SOLD_TIME_SK", "WS_SHIP_DATE_SK", "WS_ITEM_SK", "WS_BILL_CUSTOMER_SK", "WS_BILL_CDEMO_SK", "WS_BILL_HDEMO_SK", "WS_BILL_ADDR_SK", "WS_SHIP_CUSTOMER_SK", "WS_SHIP_CDEMO_SK", "WS_SHIP_HDEMO_SK", "WS_SHIP_ADDR_SK", "WS_WEB_PAGE_SK", "WS_WEB_SITE_SK", "WS_SHIP_MODE_SK", "WS_WAREHOUSE_SK", "WS_PROMO_SK", "WS_ORDER_NUMBER", "WS_QUANTITY", "WS_WHOLESALE_COST", "WS_LIST_PRICE", "WS_SALES_PRICE", "WS_EXT_DISCOUNT_AMT", "WS_EXT_SALES_PRICE", "WS_EXT_WHOLESALE_COST", "WS_EXT_LIST_PRICE", "WS_EXT_TAX", "WS_COUPON_AMT", "WS_EXT_SHIP_COST", "WS_NET_PAID", "WS_NET_PAID_INC_TAX", "WS_NET_PAID_INC_SHIP", "WS_NET_PAID_INC_SHIP_TAX", "WS_NET_PROFIT", "WS_SOLD_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_RETURNED_TIME_SK", "WR_ITEM_SK", "WR_REFUNDED_CUSTOMER_SK", "WR_REFUNDED_CDEMO_SK", "WR_REFUNDED_HDEMO_SK", "WR_REFUNDED_ADDR_SK", "WR_RETURNING_CUSTOMER_SK", "WR_RETURNING_CDEMO_SK", "WR_RETURNING_HDEMO_SK", "WR_RETURNING_ADDR_SK", "WR_WEB_PAGE_SK", "WR_REASON_SK", "WR_ORDER_NUMBER", "WR_RETURN_QUANTITY", "WR_RETURN_AMT", "WR_RETURN_TAX", "WR_RETURN_AMT_INC_TAX", "WR_FEE", "WR_RETURN_SHIP_COST", "WR_REFUNDED_CASH", "WR_REVERSED_CHARGE", "WR_ACCOUNT_CREDIT", "WR_NET_LOSS", "WR_RETURNED_DATE_SK"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("1999-02-01")), Literal(Date.valueOf("1999-04-02"))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("(CA_STATE IS NOT NULL AND (CA_STATE = ?))"),
    List(Literal("IL")),
    None,
    List()
  )),("TPCDS.WEB_SITE" -> ScanDetails(
    List("WEB_SITE_SK", "WEB_COMPANY_NAME"),
    Some("(WEB_COMPANY_NAME IS NOT NULL AND (WEB_COMPANY_NAME = ?))"),
    List(Literal("pri")),
    None,
    List()
  )))


  private def q95 = Map(("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_SHIP_DATE_SK", "WS_SHIP_ADDR_SK", "WS_WEB_SITE_SK", "WS_ORDER_NUMBER", "WS_EXT_SHIP_COST", "WS_NET_PROFIT"),
    Some("((WS_SHIP_DATE_SK IS NOT NULL AND WS_SHIP_ADDR_SK IS NOT NULL) AND WS_WEB_SITE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_WAREHOUSE_SK", "WS_ORDER_NUMBER"),
    Some("WS_WAREHOUSE_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_WAREHOUSE_SK", "WS_ORDER_NUMBER"),
    Some("WS_WAREHOUSE_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_RETURNS" -> ScanDetails(
    List("WR_ORDER_NUMBER"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_WAREHOUSE_SK", "WS_ORDER_NUMBER"),
    Some("WS_WAREHOUSE_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.WEB_SALES" -> ScanDetails(
    List("WS_WAREHOUSE_SK", "WS_ORDER_NUMBER"),
    Some("WS_WAREHOUSE_SK IS NOT NULL"),
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("1999-02-01")), Literal(Date.valueOf("1999-04-02"))),
    None,
    List()
  )),("TPCDS.CUSTOMER_ADDRESS" -> ScanDetails(
    List("CA_ADDRESS_SK", "CA_STATE"),
    Some("(CA_STATE IS NOT NULL AND (CA_STATE = ?))"),
    List(Literal("IL")),
    None,
    List()
  )),("TPCDS.WEB_SITE" -> ScanDetails(
    List("WEB_SITE_SK", "WEB_COMPANY_NAME"),
    Some("(WEB_COMPANY_NAME IS NOT NULL AND (WEB_COMPANY_NAME = ?))"),
    List(Literal("pri")),
    None,
    List()
  )))


  private def q96 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_SOLD_TIME_SK", "SS_HDEMO_SK", "SS_STORE_SK"),
    Some("((SS_HDEMO_SK IS NOT NULL AND SS_SOLD_TIME_SK IS NOT NULL) AND SS_STORE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.HOUSEHOLD_DEMOGRAPHICS" -> ScanDetails(
    List("HD_DEMO_SK", "HD_DEP_COUNT"),
    Some("(HD_DEP_COUNT IS NOT NULL AND (HD_DEP_COUNT = ?))"),
    List(Literal(Decimal(7.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.TIME_DIM" -> ScanDetails(
    List("T_TIME_SK", "T_HOUR", "T_MINUTE"),
    Some("(((T_HOUR IS NOT NULL AND T_MINUTE IS NOT NULL) AND (T_HOUR = ?)) AND (T_MINUTE >= ?))"),
    List(Literal(Decimal(20.000000000000000000, 38, 18)), Literal(Decimal(30.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.STORE" -> ScanDetails(
    List("S_STORE_SK", "S_STORE_NAME"),
    Some("(S_STORE_NAME IS NOT NULL AND (S_STORE_NAME = ?))"),
    List(Literal("ese")),
    None,
    List()
  )))


  private def q97 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )),("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_BILL_CUSTOMER_SK", "CS_ITEM_SK", "CS_SOLD_DATE_SK"),
    None,
    List(),
    Some("CS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )))


  private def q98 = Map(("TPCDS.STORE_SALES" -> ScanDetails(
    List("SS_ITEM_SK", "SS_EXT_SALES_PRICE", "SS_SOLD_DATE_SK"),
    None,
    List(),
    Some("SS_SOLD_DATE_SK IS NOT NULL"),
    List()
  )),("TPCDS.ITEM" -> ScanDetails(
    List("I_ITEM_SK", "I_ITEM_ID", "I_ITEM_DESC", "I_CURRENT_PRICE", "I_CLASS", "I_CATEGORY"),
    Some("I_CATEGORY IN ( ?, ?, ? )"),
    List(Literal("Sports"), Literal("Books"), Literal("Home")),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_DATE"),
    Some("((D_DATE IS NOT NULL AND (D_DATE >= ?)) AND (D_DATE <= ?))"),
    List(Literal(Date.valueOf("1999-02-22")), Literal(Date.valueOf("1999-03-24"))),
    None,
    List()
  )))


  private def q99 = Map(("TPCDS.CATALOG_SALES" -> ScanDetails(
    List("CS_SHIP_DATE_SK", "CS_CALL_CENTER_SK", "CS_SHIP_MODE_SK", "CS_WAREHOUSE_SK", "CS_SOLD_DATE_SK"),
    Some("(((CS_WAREHOUSE_SK IS NOT NULL AND CS_SHIP_MODE_SK IS NOT NULL) AND CS_CALL_CENTER_SK IS NOT NULL) AND CS_SHIP_DATE_SK IS NOT NULL)"),
    List(),
    None,
    List()
  )),("TPCDS.WAREHOUSE" -> ScanDetails(
    List("W_WAREHOUSE_SK", "W_WAREHOUSE_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.SHIP_MODE" -> ScanDetails(
    List("SM_SHIP_MODE_SK", "SM_TYPE"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.CALL_CENTER" -> ScanDetails(
    List("CC_CALL_CENTER_SK", "CC_NAME"),
    None,
    List(),
    None,
    List()
  )),("TPCDS.DATE_DIM" -> ScanDetails(
    List("D_DATE_SK", "D_MONTH_SEQ"),
    Some("((D_MONTH_SEQ IS NOT NULL AND (D_MONTH_SEQ >= ?)) AND (D_MONTH_SEQ <= ?))"),
    List(Literal(Decimal(1200.000000000000000000, 38, 18)), Literal(Decimal(1211.000000000000000000, 38, 18))),
    None,
    List()
  )))



  // scalastyle:on line.size.limit


  val queryOutPutMap = Map(
    "q1" -> q1,
    "q2" -> q2,
    "q3" -> q3,
    "q4" -> q4,
    "q5" -> q5,
    "q6" -> q6,
    "q7" -> q7,
    "q8" -> q8,
    "q9" -> q9,
    "q10" -> q10,
    "q11" -> q11,
    "q12" -> q12,
    "q13" -> q13,
    "q14-1" -> q14_1,
    "q14-2" -> q14_2,
    "q15" -> q15,
    "q16" -> q16,
    "q17" -> q17,
    "q18" -> q18,
    "q19" -> q19,
    "q20" -> q20,
    "q21" -> q21,
    "q22" -> q22,
    "q23-1" -> q23_1,
    "q23-2" -> q23_2,
    "q24-1" -> q24_1,
    "q24-2" -> q24_2,
    "q25" -> q25,
    "q26" -> q26,
    "q27" -> q27,
    "q28" -> q28,
    "q29" -> q29,
    "q30" -> q30,
    "q31" -> q31,
    "q32" -> q32,
    "q33" -> q33,
    "q34" -> q34,
    "q35" -> q35,
    "q36" -> q36,
    "q37" -> q37,
    "q38" -> q38,
    "q39-1" -> q39_1,
    "q39-2" -> q39_2,
    "q40" -> q40,
    "q41" -> q41,
    "q42" -> q42,
    "q43" -> q43,
    "q44" -> q44,
    "q45" -> q45,
    "q46" -> q46,
    "q47" -> q47,
    "q48" -> q48,
    "q49" -> q49,
    "q50" -> q50,
    "q51" -> q51,
    "q52" -> q52,
    "q53" -> q53,
    "q54" -> q54,
    "q55" -> q55,
    "q56" -> q56,
    "q57" -> q57,
    "q58" -> q58,
    "q59" -> q59,
    "q60" -> q60,
    "q61" -> q61,
    "q62" -> q62,
    "q63" -> q63,
    "q64" -> q64,
    "q65" -> q65,
    "q66" -> q66,
    "q67" -> q67,
    "q68" -> q68,
    "q69" -> q69,
    "q70" -> q70,
    "q71" -> q71,
    "q72" -> q72,
    "q73" -> q73,
    "q74" -> q74,
    "q75" -> q75,
    "q76" -> q76,
    "q77" -> q77,
    "q78" -> q78,
    "q79" -> q79,
    "q80" -> q80,
    "q81" -> q81,
    "q82" -> q82,
    "q83" -> q83,
    "q84" -> q84,
    "q85" -> q85,
    "q86" -> q86,
    "q87" -> q87,
    "q88" -> q88,
    "q89" -> q89,
    "q90" -> q90,
    "q91" -> q91,
    "q92" -> q92,
    "q93" -> q93,
    "q94" -> q94,
    "q95" -> q95,
    "q96" -> q96,
    "q97" -> q97,
    "q98" -> q98,
    "q99" -> q99)
}
