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

package object expressions {

  private[expressions] val MINUS = "-"
  private[expressions] val PLUS = "+"
  private[expressions] val MULTIPLY = "*"
  private[expressions] val DIVIDE = "/"
  private[expressions] val ABS = "ABS"
  private[expressions] val TRUNC = "TRUNC"
  private[expressions] val REMAINDER = "REMAINDER"
  private[expressions] val LEAST = "LEAST"
  private[expressions] val GREATEST = "GREATEST"
  private[expressions] val MOD = "MOD"
  private[expressions] val NOT = "NOT"
  private[oracle] val AND = "AND"
  private[expressions] val OR = "OR"
  private[expressions] val DECODE = "DECODE"
  private[oracle] val EQ = "="
  private[expressions] val ISNULL = "IS NULL"
  private[expressions] val ISNOTNULL = "IS NOT NULL"
  private[expressions]val IGNORE_NULLS = "ignore nulls"
  private[expressions] val RESPECT_NULLS = "respect nulls"
  private[expressions] val COALESCE = "COALESCE"
  private[expressions] val SUBSTR = "SUBSTR"
  private[expressions] val LIKE = "LIKE"
  private[expressions] val CONCAT = "CONCAT"
  private[expressions] val UPPER = "UPPER"
  private[expressions] val TRIM = "TRIM"
  private[expressions] val LEADING = "LEADING"
  private[expressions] val TRAILING = "TRAILING"
  private[expressions] val BOTH = "BOTH"

  private[expressions] val AVG = "AVG"
  private[expressions] val SUM = "SUM"
  private[expressions] val DENSE_RANK = "DENSE_RANK"
  private[expressions] val PERCENT_RANK = "PERCENT_RANK"
  private[expressions] val RANK = "RANK"
  private[expressions] val NTH_VALUE = "NTH_VALUE"
  private[expressions] val CUME_DIST = "CUME_DIST"
  private[expressions] val NTILE = "NTILE"
  private[expressions] val ROW_NUMBER = "ROW_NUMBER"
  private[expressions] val COUNT = "COUNT"
  private[expressions] val FIRST_VALUE = "FIRST_VALUE"
  private[expressions] val LAST_VALUE = "LAST_VALUE"
  private[expressions] val STDDEV_POP = "STDDEV_POP"
  private[expressions] val STDDEV_SAMP = "STDDEV_SAMP"
  private[expressions] val VAR_POP = "VAR_POP"
  private[expressions] val VAR_SAMP = "VAR_SAMP"
  private[expressions] val COVAR_POP = "COVAR_POP"
  private[expressions] val COVAR_SAMP = "COVAR_SAMP"
  private[expressions] val CORR = "CORR"
  private[expressions] val MIN = "MIN"
  private[expressions] val MAX = "MAX"
}
