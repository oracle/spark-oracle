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

package org.apache.spark.sql

import org.apache.spark.sql.hive.test.oracle.TestOracleHive

class MacrosTest extends MacrosAbstractTest {

  import org.apache.spark.sql.defineMacros._
  import org.apache.spark.sql.catalyst.ScalaReflection._
  import universe._

  test("compileTime") { td =>
    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => i))

    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => i + 1))

    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => {
      val j = 5
      j
    }))

    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => {
      val b = Array(5)
      val j = 5
      j
    }))
  }

  test("basics") {td =>

    handleMacroOutput(eval(q"(i : Int) => i"))

    handleMacroOutput(eval(q"(i : java.lang.Integer) => i"))

//    val a = Array(5)
//    handleMacroOutput(eval[Int, Int](q"(i : Int) => a(0)"))

    handleMacroOutput(eval(q"(i : Int) => i + 5"))

    handleMacroOutput(eval(
      q"""{(i : Int) =>
          val b = Array(5)
          val j = 5
          j
        }"""))

    handleMacroOutput(eval(reify {
      (i : Int) => org.apache.spark.SPARK_BRANCH.length + i
    }.tree))

    handleMacroOutput(eval(reify {(i : Int) =>
      val b = Array(5, 6)
      val j = b(0)
      i + j + Math.abs(j)}.tree))

    handleMacroOutput(eval(
      q"""{(i : Int) =>
          val b = Array(5)
          val j = 5
          j
        }"""))
  }

  test("udts") {td =>
    import macrotest.ExampleStructs.Point
    handleMacroOutput(eval(
      reify {(p : Point) =>
          Point(1, 2)
        }.tree
    )
    )

    handleMacroOutput(eval(
      reify {(p : Point) =>
        p.x + p.y
      }.tree
    )
    )

    handleMacroOutput(eval(
      reify {(p : Point) =>
        Point(p.x + p.y, p.y)
      }.tree
    )
    )
  }

  test("optimizeExpr") { td =>
    import macrotest.ExampleStructs.Point
    handleMacroOutput(eval(
      reify {(p : Point) =>
        val p1 = Point(p.x, p.y)
        val a = Array(1)
        val m = Map(1 -> 2)
        p1.x + p1.y + a(0) + m(1)
      }.tree
    )
    )
  }

  test("tuples") {td =>
    handleMacroOutput(eval(
      reify {(t : Tuple2[Int, Int]) =>
        (t._2, t._1)
      }.tree
    )
    )

    handleMacroOutput(eval(
      reify {(t : Tuple2[Int, Int]) =>
        t._2 -> t._1
      }.tree
    )
    )

    handleMacroOutput(eval(
      reify {(t : Tuple4[Float, Double, Int, Int]) =>
        (t._4 + t._3, t._4)
      }.tree
    )
    )
  }

  test("arrays") {td =>
    handleMacroOutput(eval(
      reify {(i : Int) =>
          val b = Array(5, i)
          val j = b(0)
          j + b(1)
        }.tree))
  }

  test("maps") {td =>
    handleMacroOutput(eval(
      reify {(i : Int) =>
          val b = Map(0 -> i, 1 -> (i + 1))
          val j = b(0)
          j + b(1)
        }.tree))
  }

  test("datetimes") {td =>
    import java.sql.Date
    import java.sql.Timestamp
    import java.time.ZoneId
    import java.time.Instant
    import org.apache.spark.unsafe.types.CalendarInterval
    import org.apache.spark.sql.sqlmacros.DateTimeUtils._

    handleMacroOutput(eval(
      reify {(dt : Date) =>
        val dtVal = dt
        val dtVal2 = new Date(System.currentTimeMillis())
        val tVal = new Timestamp(System.currentTimeMillis())
        val dVal3 = localDateToDays(java.time.LocalDate.of(2000, 1, 1))
        val t2 = instantToMicros(Instant.now())
        val t3 = stringToTimestamp("2000-01-01", ZoneId.systemDefault()).get
        val t4 = daysToMicros(dtVal, ZoneId.systemDefault())
        getDayInYear(dtVal) + getDayOfMonth(dtVal) + getDayOfWeek(dtVal2) +
          getHours(tVal, ZoneId.systemDefault) + getSeconds(t2, ZoneId.systemDefault) +
          getMinutes(t3, ZoneId.systemDefault()) +
          getDayInYear(dateAddMonths(dtVal, getMonth(dtVal2))) +
          getDayInYear(dVal3) +
          getHours(
            timestampAddInterval(t4, new CalendarInterval(1, 1, 1), ZoneId.systemDefault()),
            ZoneId.systemDefault) +
          getDayInYear(dateAddInterval(dtVal, new CalendarInterval(1, 1, 1L))) +
          monthsBetween(t2, t3, true, ZoneId.systemDefault()) +
          getDayOfMonth(getNextDateForDayOfWeek(dtVal2, "MO")) +
          getDayInYear(getLastDayOfMonth(dtVal2)) + getDayOfWeek(truncDate(dtVal, "week")) +
          getHours(toUTCTime(t3, ZoneId.systemDefault().toString), ZoneId.systemDefault())
      }.tree))
  }

  test("taxAndDiscount") { td =>
    import org.apache.spark.sql.sqlmacros.DateTimeUtils._
    import java.sql.Date
    import java.time.ZoneId

    handleMacroOutput(eval(
      reify { (prodCat : String, amt : Double) =>
        val taxRate = prodCat match {
          case "grocery" => 0.0
          case "alcohol" => 10.5
          case _ => 9.5
        }
        val currDate = currentDate(ZoneId.systemDefault())
        val discount = if (getDayOfWeek(currDate) == 1 && prodCat == "alcohol") 0.05 else 0.0

        amt * ( 1.0 - discount) * (1.0 + taxRate)

      }.tree))
  }

  test("taxAndDiscountMultiMacro") { td =>
    import org.apache.spark.sql.sqlmacros.DateTimeUtils._
    import java.sql.Date
    import java.time.ZoneId

    import org.apache.spark.sql.sqlmacros.registered_macros

    register("taxRate", reify {(prodCat : String) =>
      prodCat match {
        case "grocery" => 0.0
        case "alcohol" => 10.5
        case _ => 9.5
      }
    }.tree)

    register("discount", reify {(prodCat : String) =>
      val currDate = currentDate(ZoneId.systemDefault())
      if (getDayOfWeek(currDate) == 1 && prodCat == "alcohol") 0.05 else 0.0
    }.tree)

    register("taxAndDiscount", reify {(prodCat : String, amt : Double) =>
      val taxRate : Double = registered_macros.taxRate(prodCat)
      val discount : Double = registered_macros.discount(prodCat)
      amt * ( 1.0 - discount) * (1.0 + taxRate)
    }.tree)

    val dfM =
      TestOracleHive.sql(
        "select taxAndDiscount(c_varchar2_40, c_number) from sparktest.unit_test"
      )
    printOut(
      s"""Macro based Plan:
         |${dfM.queryExecution.analyzed}""".stripMargin
    )

  }

  test("conditionals") { td =>
    import org.apache.spark.sql.sqlmacros.PredicateUtils._
    import macrotest.ExampleStructs.Point

    handleMacroOutput(eval(
      reify { (i: Int) =>
        val j = if (i > 7 && i < 20 && i.is_not_null) {
          i
        } else if (i == 6 || i.in(4, 5) ) {
          i + 1
        } else i + 2
        val k = i match {
          case 1 => i + 2
          case _ => i + 3
        }
        val l = (j, k) match {
          case (1, 2) => 1
          case (3, 4) => 2
          case _ => 3
        }
        val p = Point(k, l)
        val m = p match {
          case Point(1, 2) => 1
          case _ => 2
        }
        j + k + l + m
      }.tree))

    handleMacroOutput(eval(
      reify { (s: String) =>
        val i = if (s.endsWith("abc")) 1 else 0
        val j = if (s.contains("abc")) 1 else 0
        val k = if (s.is_not_null && s.not_in("abc")) 1 else 0
        i + j + k
      }.tree))
  }

  test("macroVsFuncPlan") { td =>

    TestOracleHive.sparkSession.registerMacro("fnM", {
      TestOracleHive.sparkSession.udm((i: Int) => i + 1)
    })

    TestOracleHive.udf.register("fn", (i: Int) => i + 1)

    val dfM = TestOracleHive.sql("select fnM(c_int) from sparktest.unit_test")
    printOut(
      s"""Macro based Plan:
         |${dfM.queryExecution.analyzed}""".stripMargin
    )

    val dfF = TestOracleHive.sql("select fn(c_int) from sparktest.unit_test")
    printOut(
      s"""Function based Plan:
         |${dfF.queryExecution.analyzed}""".stripMargin
    )
  }

  test("macroPlan") { td =>

    import TestOracleHive.sparkSession.implicits._

    TestOracleHive.sparkSession.registerMacro("m1",
      TestOracleHive.sparkSession.udm(
        {(i : Int) =>
          val b = Array(5, 6)
          val j = b(0)
          val k = new java.sql.Date(System.currentTimeMillis()).getTime
          i + j + k + Math.abs(j)
        }
      )
    )

    val dfM = TestOracleHive.sql("select m1(c_int) from sparktest.unit_test")
    printOut(
      s"""Macro based Plan:
         |${dfM.queryExecution.analyzed}""".stripMargin
    )

  }

  test("macroWithinMacro") { td =>

    import org.apache.spark.sql.sqlmacros.registered_macros

    register("m2", reify {(i : Int) =>
      val b = Array(5, 6)
      val j = b(0)
      val k = new java.sql.Date(System.currentTimeMillis()).getTime
      i + j + k + Math.abs(j)
    }.tree)

    register("m3", reify {(i : Int) =>
      val l : Int = registered_macros.m2(i)
      i + l
    }.tree)

    val dfM = TestOracleHive.sql("select m3(c_int) from sparktest.unit_test")
    printOut(
      s"""Macro based Plan:
         |${dfM.queryExecution.analyzed}""".stripMargin
    )
  }

}
