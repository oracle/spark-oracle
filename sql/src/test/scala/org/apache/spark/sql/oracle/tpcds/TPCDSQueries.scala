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
import org.apache.spark.sql.oracle.BasicExplainTest
import org.apache.spark.sql.oracle.operators.OraTableScanValidator.ScanDetails
import org.apache.spark.sql.oracle.tpcds.{TPCDSQueryOutPutMap => out}
import org.apache.spark.sql.oracle.tpcds.{TPCDSQueryMap => qry}
import org.apache.spark.sql.types.Decimal


// scalastyle:off println
// scalastyle:off line.size.limit
object TPCDSQueries {

  case class TPCDSQuerySpec(sql: String, scanDetailsMap: Map[String, ScanDetails])

  import QuerySet1._

  val queries = Seq(
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
    "q99" -> q99
      )

  object QuerySet1 {
    val q1 = TPCDSQuerySpec(qry.q1, out.queryOutPutMap("q1"))
    val q2 = TPCDSQuerySpec(qry.q2, out.queryOutPutMap("q2"))
    val q3 = TPCDSQuerySpec(qry.q3, out.queryOutPutMap("q3"))
    val q4 = TPCDSQuerySpec(qry.q4, out.queryOutPutMap("q4"))
    val q5 = TPCDSQuerySpec(qry.q5, out.queryOutPutMap("q5"))
    val q6 = TPCDSQuerySpec(qry.q6, out.queryOutPutMap("q6"))
    val q7 = TPCDSQuerySpec(qry.q7, out.queryOutPutMap("q7"))
    val q8 = TPCDSQuerySpec(qry.q8, out.queryOutPutMap("q8"))
    val q9 = TPCDSQuerySpec(qry.q9, out.queryOutPutMap("q9"))
    val q10 = TPCDSQuerySpec(qry.q10, out.queryOutPutMap("q10"))
    val q11 = TPCDSQuerySpec(qry.q11, out.queryOutPutMap("q11"))
    val q12 = TPCDSQuerySpec(qry.q12, out.queryOutPutMap("q12"))
    val q13 = TPCDSQuerySpec(qry.q13, out.queryOutPutMap("q13"))
    val q14_1 = TPCDSQuerySpec(qry.q14_1, out.queryOutPutMap("q14-1"))
    val q14_2 = TPCDSQuerySpec(qry.q14_2, out.queryOutPutMap("q14-2"))
    val q15 = TPCDSQuerySpec(qry.q15, out.queryOutPutMap("q15"))
    val q16 = TPCDSQuerySpec(qry.q16, out.queryOutPutMap("q16"))
    val q17 = TPCDSQuerySpec(qry.q17, out.queryOutPutMap("q17"))
    val q18 = TPCDSQuerySpec(qry.q18, out.queryOutPutMap("q18"))
    val q19 = TPCDSQuerySpec(qry.q19, out.queryOutPutMap("q19"))
    val q20 = TPCDSQuerySpec(qry.q20, out.queryOutPutMap("q20"))
    val q21 = TPCDSQuerySpec(qry.q21, out.queryOutPutMap("q21"))
    val q22 = TPCDSQuerySpec(qry.q22, out.queryOutPutMap("q22"))
    val q23_1 = TPCDSQuerySpec(qry.q23_1, out.queryOutPutMap("q23-1"))
    val q23_2 = TPCDSQuerySpec(qry.q23_2, out.queryOutPutMap("q23-2"))
    val q24_1 = TPCDSQuerySpec(qry.q24_1, out.queryOutPutMap("q24-1"))
    val q24_2 = TPCDSQuerySpec(qry.q24_2, out.queryOutPutMap("q24-2"))
    val q25 = TPCDSQuerySpec(qry.q25, out.queryOutPutMap("q25"))
    val q26 = TPCDSQuerySpec(qry.q26, out.queryOutPutMap("q26"))
    val q27 = TPCDSQuerySpec(qry.q27, out.queryOutPutMap("q27"))
    val q28 = TPCDSQuerySpec(qry.q28, out.queryOutPutMap("q28"))
    val q29 = TPCDSQuerySpec(qry.q29, out.queryOutPutMap("q29"))
    val q30 = TPCDSQuerySpec(qry.q30, out.queryOutPutMap("q30"))
    val q31 = TPCDSQuerySpec(qry.q31, out.queryOutPutMap("q31"))
    val q32 = TPCDSQuerySpec(qry.q32, out.queryOutPutMap("q32"))
    val q33 = TPCDSQuerySpec(qry.q33, out.queryOutPutMap("q33"))
    val q34 = TPCDSQuerySpec(qry.q34, out.queryOutPutMap("q34"))
    val q35 = TPCDSQuerySpec(qry.q35, out.queryOutPutMap("q35"))
    val q36 = TPCDSQuerySpec(qry.q36, out.queryOutPutMap("q36"))
    val q37 = TPCDSQuerySpec(qry.q37, out.queryOutPutMap("q37"))
    val q38 = TPCDSQuerySpec(qry.q38, out.queryOutPutMap("q38"))
    val q39_1 = TPCDSQuerySpec(qry.q39_1, out.queryOutPutMap("q39-1"))
    val q39_2 = TPCDSQuerySpec(qry.q39_2, out.queryOutPutMap("q39-2"))
    val q40 = TPCDSQuerySpec(qry.q40, out.queryOutPutMap("q40"))
    val q41 = TPCDSQuerySpec(qry.q41, out.queryOutPutMap("q41"))
    val q42 = TPCDSQuerySpec(qry.q42, out.queryOutPutMap("q42"))
    val q43 = TPCDSQuerySpec(qry.q43, out.queryOutPutMap("q43"))
    val q44 = TPCDSQuerySpec(qry.q44, out.queryOutPutMap("q44"))
    val q45 = TPCDSQuerySpec(qry.q45, out.queryOutPutMap("q45"))
    val q46 = TPCDSQuerySpec(qry.q46, out.queryOutPutMap("q46"))
    val q47 = TPCDSQuerySpec(qry.q47, out.queryOutPutMap("q47"))
    val q48 = TPCDSQuerySpec(qry.q48, out.queryOutPutMap("q48"))
    val q49 = TPCDSQuerySpec(qry.q49, out.queryOutPutMap("q49"))
    val q50 = TPCDSQuerySpec(qry.q50, out.queryOutPutMap("q50"))
    val q51 = TPCDSQuerySpec(qry.q51, out.queryOutPutMap("q51"))
    val q52 = TPCDSQuerySpec(qry.q52, out.queryOutPutMap("q52"))
    val q53 = TPCDSQuerySpec(qry.q53, out.queryOutPutMap("q53"))
    val q54 = TPCDSQuerySpec(qry.q54, out.queryOutPutMap("q54"))
    val q55 = TPCDSQuerySpec(qry.q55, out.queryOutPutMap("q55"))
    val q56 = TPCDSQuerySpec(qry.q56, out.queryOutPutMap("q56"))
    val q57 = TPCDSQuerySpec(qry.q57, out.queryOutPutMap("q57"))
    val q58 = TPCDSQuerySpec(qry.q58, out.queryOutPutMap("q58"))
    val q59 = TPCDSQuerySpec(qry.q59, out.queryOutPutMap("q59"))
    val q60 = TPCDSQuerySpec(qry.q60, out.queryOutPutMap("q60"))
    val q61 = TPCDSQuerySpec(qry.q61, out.queryOutPutMap("q61"))
    val q62 = TPCDSQuerySpec(qry.q62, out.queryOutPutMap("q62"))
    val q63 = TPCDSQuerySpec(qry.q63, out.queryOutPutMap("q63"))
    val q64 = TPCDSQuerySpec(qry.q64, out.queryOutPutMap("q64"))
    val q65 = TPCDSQuerySpec(qry.q65, out.queryOutPutMap("q65"))
    val q66 = TPCDSQuerySpec(qry.q66, out.queryOutPutMap("q66"))
    val q67 = TPCDSQuerySpec(qry.q67, out.queryOutPutMap("q67"))
    val q68 = TPCDSQuerySpec(qry.q68, out.queryOutPutMap("q68"))
    val q69 = TPCDSQuerySpec(qry.q69, out.queryOutPutMap("q69"))
    val q70 = TPCDSQuerySpec(qry.q70, out.queryOutPutMap("q70"))
    val q71 = TPCDSQuerySpec(qry.q71, out.queryOutPutMap("q71"))
    val q72 = TPCDSQuerySpec(qry.q72, out.queryOutPutMap("q72"))
    val q73 = TPCDSQuerySpec(qry.q73, out.queryOutPutMap("q73"))
    val q74 = TPCDSQuerySpec(qry.q74, out.queryOutPutMap("q74"))
    val q75 = TPCDSQuerySpec(qry.q75, out.queryOutPutMap("q75"))
    val q76 = TPCDSQuerySpec(qry.q76, out.queryOutPutMap("q76"))
    val q77 = TPCDSQuerySpec(qry.q77, out.queryOutPutMap("q77"))
    val q78 = TPCDSQuerySpec(qry.q78, out.queryOutPutMap("q78"))
    val q79 = TPCDSQuerySpec(qry.q79, out.queryOutPutMap("q79"))
    val q80 = TPCDSQuerySpec(qry.q80, out.queryOutPutMap("q80"))
    val q81 = TPCDSQuerySpec(qry.q81, out.queryOutPutMap("q81"))
    val q82 = TPCDSQuerySpec(qry.q82, out.queryOutPutMap("q82"))
    val q83 = TPCDSQuerySpec(qry.q83, out.queryOutPutMap("q83"))
    val q84 = TPCDSQuerySpec(qry.q84, out.queryOutPutMap("q84"))
    val q85 = TPCDSQuerySpec(qry.q85, out.queryOutPutMap("q85"))
    val q86 = TPCDSQuerySpec(qry.q86, out.queryOutPutMap("q86"))
    val q87 = TPCDSQuerySpec(qry.q87, out.queryOutPutMap("q87"))
    val q88 = TPCDSQuerySpec(qry.q88, out.queryOutPutMap("q88"))
    val q89 = TPCDSQuerySpec(qry.q89, out.queryOutPutMap("q89"))
    val q90 = TPCDSQuerySpec(qry.q90, out.queryOutPutMap("q90"))
    val q91 = TPCDSQuerySpec(qry.q91, out.queryOutPutMap("q91"))
    val q92 = TPCDSQuerySpec(qry.q92, out.queryOutPutMap("q92"))
    val q93 = TPCDSQuerySpec(qry.q93, out.queryOutPutMap("q93"))
    val q94 = TPCDSQuerySpec(qry.q94, out.queryOutPutMap("q94"))
    val q95 = TPCDSQuerySpec(qry.q95, out.queryOutPutMap("q95"))
    val q96 = TPCDSQuerySpec(qry.q96, out.queryOutPutMap("q96"))
    val q97 = TPCDSQuerySpec(qry.q97, out.queryOutPutMap("q97"))
    val q98 = TPCDSQuerySpec(qry.q98, out.queryOutPutMap("q98"))
    val q99 = TPCDSQuerySpec(qry.q99, out.queryOutPutMap("q99"))
  }
}
