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

package org.apache.spark.sql.oracle.testutils

import java.math.MathContext
import java.sql.{Date, Timestamp}

/**
 *
 * Utility to generate values of different datatypes.
 * Uses [[org.scalacheck.Gen]]
 *
 * Note:
 * 1. Gen.listOfN says at most list of size N.
 *    But looks like in most cases it generates a list of size N
 *    Assuming that in our case lists are of size N.
 *    Otherwise things break.
 */
object DataGens {

  import org.scalacheck.Gen
  import org.scalacheck.Gen._
  import org.scalacheck.Arbitrary._

  val not_null = const(false)
  val null_percent_1 = frequency((99, false), (1, true))
  val null_percent_15 = frequency((85, false), (15, true))
  val choose_null_distribution = oneOf(not_null, null_percent_1, null_percent_15)

  def withNullFlag[T](g : Gen[T],
                      nullGen : Gen[Boolean] = choose_null_distribution
                     ) : Gen[(T, Boolean)] =
    for (
      v <- g;
      isNull <- nullGen
    ) yield (v, isNull)

  private def strFromChars(sz : Int, charGen : Gen[Char]) : Gen[String] = {
    listOfN(sz, charGen).map(l => String.valueOf(l.toArray))
  }

  private def varStrFromChars(maxSz : Int, charGen : Gen[Char]) : Gen[String] = {
    val strs : Seq[Gen[String]] =
      (0 until maxSz) map (sz => listOfN(sz + 1, charGen).map(l => String.valueOf(l.toArray)))
    if (maxSz == 1) {
      strs.head
    } else if (maxSz == 2) {
      oneOf(strs.head, strs.last)
    } else {
      oneOf(strs.head, strs.tail.head, strs.tail.tail: _*)
    }
  }

  /*
     Use 'asciiPrintableChar' and not 'asciiChar'
     because XMLParser not able to handle non-ascii
   */
  def sql_char(sz : Int) : Gen[String] = strFromChars(sz, alphaNumChar)

  /*
     Use 'asciiPrintableChar' and not 'arbitrary[Char]'
     because XMLParser not able to handle non-ascii
   */
  def sql_nchar(sz : Int) : Gen[String] = varStrFromChars(sz, alphaNumChar)

  /*
     Use 'asciiPrintableChar' and not 'asciiChar'
     because XMLParser not able to handle non-ascii
   */
  def sql_varchar(maxSz : Int) : Gen[String] = varStrFromChars(maxSz, alphaNumChar)

  /*
     Use 'asciiPrintableChar' and not 'arbitrary[Char]'
     because XMLParser not able to handle non-ascii
   */
  def sql_nvarchar(maxSz : Int) : Gen[String] = varStrFromChars(maxSz, alphaNumChar)

  def sql_number_wrong(prec : Int, scale : Int) : Gen[java.math.BigDecimal] = {
    val mc = new MathContext(prec, java.math.RoundingMode.HALF_EVEN)
    if (scale == 0) {
      val numGen = if (prec <= 2) {
        arbByte.arbitrary.map(_.toLong)
      } else if (prec <= 4) {
        arbShort.arbitrary.map(_.toLong)
      } else if (prec <= 9) {
        arbInt.arbitrary.map(_.toLong)
      } else {
        arbBigInt.arbitrary.map(_.toLong)
      }
      for (
        l <- numGen
      ) yield {
        new java.math.BigDecimal(l, mc)
      }
    } else {
      for (
        d <- arbDouble.arbitrary
      ) yield {
        var bd = BigDecimal.decimal(d, mc)
        if (scale > 0) {
          if (bd.scale > scale) {
            bd = bd.setScale(scale, BigDecimal.RoundingMode.HALF_EVEN)
          }
        }
        bd.bigDecimal
      }
    }
  }

  def sql_number(prec : Int, scale : Int) : Gen[java.math.BigDecimal] = {
    val mc = new MathContext(prec, java.math.RoundingMode.HALF_EVEN)
    for (
      l <- listOfN(prec - scale, numChar);
      r <- listOfN(scale, numChar);
      s <- oneOf("+", "-")
    ) yield {
      new java.math.BigDecimal(s"${s}${String.valueOf(l.toArray)}.${String.valueOf(r.toArray)}", mc)
    }
  }

  private val (min_date, max_date) = (
    java.sql.Date.valueOf("1975-01-01").getTime,
    java.sql.Date.valueOf("2030-12-31").getTime
  )

  def sql_date : Gen[Date] = {
    for (
      offset <- Gen.choose(0L, max_date - min_date)
    ) yield new java.sql.Date(min_date + offset)
  }

  def sql_timsetamp : Gen[Timestamp] = {
    for (
      offset <- Gen.choose(0L, max_date - min_date);
      nanos <- Gen.choose(0, 999999999)
    ) yield {
      val ts = new Timestamp(min_date + offset)
      ts.setNanos(nanos)
      ts
    }
  }

}
