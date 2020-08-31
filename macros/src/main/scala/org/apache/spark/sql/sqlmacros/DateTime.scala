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
package org.apache.spark.sql.sqlmacros

import java.time.ZoneId

import org.apache.spark.sql.catalyst.{expressions => sparkexpr, CatalystTypeConverters}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval


/**
 * Handle translation of ''Date, Timestamp and Interval'' values. We support
 * translation of functions from [[MacroDateTimeUtils]] module.
 *
 * ''Construction support:''
 * - `new java.sql.Date(epochVal)` is translated to
 *   `Cast(MillisToTimestamp(spark_expr(epochVal)), DateType)`
 * - `new java.sql.Timestamp(epochVal` is translated to `MillisToTimestamp(spark_expr(epochVal))`
 * - `java.time.LocalDate.of(yr, month, dayOfMonth)` is translated to
 *   `MakeDate(spark_expr(yr), spark_expr(month), spark_expr(dayOfMonth))`
 * - we don't support translation of construction of `java.time.Instant`
 *   because there is no spark expression to construct a Date value from
 *   `long epochSecond, int nanos`.
 *
 * ''Certain Arguments must be macro compile time static values:''
 * - there is no spark expression to construct a [[CalendarInterval]], so these
 *   must be static values.
 * - [[ZoneId]] must be a static value.
 */
trait DateTime { self: ExprTranslator =>

  import macroUniverse._

  val dateTyp = typeOf[java.sql.Date]
  val localDateTyp = typeOf[java.time.LocalDate]
  val timestampTyp = typeOf[java.sql.Timestamp]
  val instantTyp = typeOf[java.time.Instant]
  val calIntervalTyp = typeOf[CalendarInterval]
  val zoneIdTyp = typeOf[ZoneId]

  val dateConstructors = dateTyp.member(termNames.CONSTRUCTOR).alternatives
  val timeStampConstructors = timestampTyp.member(termNames.CONSTRUCTOR).alternatives
  val localDteOfMethod = localDateTyp.companion.member(TermName("of")).
    alternatives.filter(m => m.asMethod.paramLists(0)(1).typeSignature =:= typeOf[Int])

  val dateTimeUtils = typeOf[DateTimeUtils.type]

  val fromJavaDateSym = dateTimeUtils.member(TermName("fromJavaDate"))
  val fromJavaTimestamp = dateTimeUtils.member(TermName("fromJavaTimestamp"))
  val instantToMicrosSym = dateTimeUtils.member(TermName("instantToMicros"))
  val localDateToDaysSym = dateTimeUtils.member(TermName("localDateToDays"))

  val microsToDaysSym = dateTimeUtils.member(TermName("microsToDays"))
  val daysToMicrosSym = dateTimeUtils.member(TermName("daysToMicros"))
  val millisToMicrosSym = dateTimeUtils.member(TermName("millisToMicros"))

  val stringToTimestampSym = dateTimeUtils.member(TermName("stringToTimestamp"))
  val stringToTimestampAnsiSym = dateTimeUtils.member(TermName("stringToTimestampAnsi"))
  val stringToDateSym = dateTimeUtils.member(TermName("stringToDate"))

  val getHoursSym = dateTimeUtils.member(TermName("getHours"))
  val getMinutesSym = dateTimeUtils.member(TermName("getMinutes"))
  val getSecondsSym = dateTimeUtils.member(TermName("getSeconds"))
  val getSecondsWithFractionSym = dateTimeUtils.member(TermName("getSecondsWithFraction"))
  // val getMicrosecondsSym = dateTimeUtils.member(TermName("getMicroseconds"))
  val getDayInYearSym = dateTimeUtils.member(TermName("getDayInYear"))
  val getYearSym = dateTimeUtils.member(TermName("getYear"))
  val getWeekBasedYearSym = dateTimeUtils.member(TermName("getWeekBasedYear"))
  val getQuarterSym = dateTimeUtils.member(TermName("getQuarter"))
  val getMonthSym = dateTimeUtils.member(TermName("getMonth"))
  val getDayOfMonthSym = dateTimeUtils.member(TermName("getDayOfMonth"))
  val getDayOfWeekSym = dateTimeUtils.member(TermName("getDayOfWeek"))
  val getWeekDaySym = dateTimeUtils.member(TermName("getWeekDay"))
  val getWeekOfYearSym = dateTimeUtils.member(TermName("getWeekOfYear"))

  val dateAddMonths = dateTimeUtils.member(TermName("dateAddMonths"))
  val timestampAddIntervalSym = dateTimeUtils.member(TermName("timestampAddInterval"))
  val dateAddIntervalSym = dateTimeUtils.member(TermName("dateAddInterval"))
  val monthsBetweenSym = dateTimeUtils.member(TermName("monthsBetween"))
  val getNextDateForDayOfWeekSym = dateTimeUtils.member(TermName("getNextDateForDayOfWeek"))
  val getLastDayOfMonth = dateTimeUtils.member(TermName("getLastDayOfMonth"))
  val truncDateSym = dateTimeUtils.member(TermName("truncDate"))
  val truncTimestampSym = dateTimeUtils.member(TermName("truncTimestamp"))
  val fromUTCTimeSym = dateTimeUtils.member(TermName("fromUTCTimeSym"))
  val toUTCTimeSym = dateTimeUtils.member(TermName("toUTCTime"))
  val currentTimestampSym = dateTimeUtils.member(TermName("currentTimestamp"))
  val currentDateSym = dateTimeUtils.member(TermName("currentDate"))

  private val dayOfWeekMap = {
    // from DateTimeUtils
    val SUNDAY = 3
    val MONDAY = 4
    val TUESDAY = 5
    val WEDNESDAY = 6
    val THURSDAY = 0
    val FRIDAY = 1
     val SATURDAY = 2
    sparkexpr.Literal(
      CatalystTypeConverters.convertToCatalyst(
    Map(SUNDAY -> "SU", MONDAY -> "MO", TUESDAY -> "TU", WEDNESDAY -> "WE",
      THURSDAY -> "TH", FRIDAY -> "FR", SATURDAY -> "SA")
      ), MapType(IntegerType, StringType)
    )
  }

  private val truncLevelMap = {
    val TRUNC_TO_MICROSECOND = 0
    val TRUNC_TO_MILLISECOND = 1
    val TRUNC_TO_SECOND = 2
    val TRUNC_TO_MINUTE = 3
    val TRUNC_TO_HOUR = 4
    val TRUNC_TO_DAY = 5
    val TRUNC_TO_WEEK = 6
    val TRUNC_TO_MONTH = 7
    val TRUNC_TO_QUARTER = 8
    val TRUNC_TO_YEAR = 9
    sparkexpr.Literal(
      CatalystTypeConverters.convertToCatalyst(
        Map(
          TRUNC_TO_MICROSECOND -> "MICROSECOND",
          TRUNC_TO_MILLISECOND -> "MILLISECOND",
          TRUNC_TO_SECOND -> "SECOND",
          TRUNC_TO_MINUTE -> "MINUTE",
          TRUNC_TO_HOUR -> "HOUR",
          TRUNC_TO_DAY -> "DAY",
          TRUNC_TO_WEEK -> "WEEK",
          TRUNC_TO_MONTH -> "MON",
          TRUNC_TO_QUARTER -> "QUARTER",
          TRUNC_TO_YEAR -> "YEAR"
        )
      ), MapType(IntegerType, StringType)
    )
  }

  object DateTimePatterns {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      t match {
        case q"new $id(..$args)" if dateConstructors.contains(t.symbol) && args.size == 1 =>
          for (entries <- CatalystExpressions.unapplySeq(args))
            yield sparkexpr.Cast(sparkexpr.MillisToTimestamp(entries.head), DateType)
        case q"new $id(..$args)" if timeStampConstructors.contains(t.symbol) && args.size == 1 =>
          for (entries <- CatalystExpressions.unapplySeq(args))
            yield sparkexpr.MillisToTimestamp(entries.head)
        case q"$id(..$args)" =>
          id match {
            case id if localDteOfMethod.contains(id.symbol) =>
              for (entries <- CatalystExpressions.unapplySeq(args)) yield
              sparkexpr.MakeDate(entries(0), entries(1), entries(2))
            case id if id.symbol == microsToDaysSym =>
              for (expr <- CatalystExpression.unapply(args.head);
                   zId <- zoneId(args.tail.head))
                yield sparkexpr.Cast(expr, DateType, Some(zId.toString))
            case id if id.symbol == daysToMicrosSym =>
              for (expr <- CatalystExpression.unapply(args.head);
                   zId <- zoneId(args.tail.head))
                yield sparkexpr.Cast(expr, TimestampType, Some(zId.toString))
            /*
             * Child is already a internal Date/Timestamp form, so no conversion needed
             */
            case id if id.symbol == instantToMicrosSym =>
              for (entries <- CatalystExpressions.unapplySeq(args)) yield entries.head
            case id if id.symbol == fromJavaDateSym =>
              for (entries <- CatalystExpressions.unapplySeq(args)) yield entries.head
            case id if id.symbol == fromJavaTimestamp =>
              for (entries <- CatalystExpressions.unapplySeq(args)) yield entries.head
            case id if id.symbol == localDateToDaysSym =>
              for (entries <- CatalystExpressions.unapplySeq(args)) yield entries.head
            /*
             * End no conversion needed
             */
            case id if id.symbol == millisToMicrosSym =>
              for (entries <- CatalystExpressions.unapplySeq(args))
                yield sparkexpr.MillisToTimestamp(entries.head)
            case id if id.symbol == stringToTimestampSym =>
              for (strExpr <- CatalystExpression.unapply(args.head);
                   zId <- zoneId(args.tail.head)) yield
                sparkexpr.objects.WrapOption(
                  sparkexpr.Cast(strExpr, TimestampType, Some(zId.toString)),
                  TimestampType
                )
            case id if id.symbol == stringToTimestampAnsiSym =>
              for (strExpr <- CatalystExpression.unapply(args.head);
                   zId <- zoneId(args.tail.head))
                yield sparkexpr.AnsiCast(strExpr, TimestampType, Some(zId.toString))
            case id if id.symbol == stringToDateSym =>
              for (strExpr <- CatalystExpression.unapply(args.head);
                   zId <- zoneId(args.tail.head))
                yield sparkexpr.objects.WrapOption(
                  sparkexpr.Cast(strExpr, DateType, Some(zId.toString)),
                  DateType)
            case id if id.symbol == getHoursSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.Hour(expr)
            case id if id.symbol == getMinutesSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.Minute(expr)
            case id if id.symbol == getSecondsSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.Second(expr)
            case id if id.symbol == getSecondsWithFractionSym =>
              for (expr <- CatalystExpression.unapply(args.head);
                   zId <- zoneId(args.tail.head))
                yield sparkexpr.SecondWithFraction(expr, Some(zId.toString))
            case id if id.symbol == getDayInYearSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.DayOfYear(expr)
            case id if id.symbol == getYearSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.Year(expr)
            case id if id.symbol == getWeekBasedYearSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.YearOfWeek(expr)
            case id if id.symbol == getQuarterSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.Quarter(expr)
            case id if id.symbol == getMonthSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.Month(expr)
            case id if id.symbol == getDayOfMonthSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.DayOfMonth(expr)
            case id if id.symbol == getDayOfWeekSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.DayOfWeek(expr)
            case id if id.symbol == getWeekDaySym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.WeekDay(expr)
            case id if id.symbol == getWeekOfYearSym =>
              for (expr <- CatalystExpression.unapply(args.head)) yield sparkexpr.WeekOfYear(expr)
            case id if id.symbol == dateAddMonths =>
              for (entries <- CatalystExpressions.unapplySeq(args))
                yield sparkexpr.AddMonths(entries.head, entries.tail.head)
            case id if id.symbol == timestampAddIntervalSym =>
              for (expr <- CatalystExpression.unapply(args(0));
                   calInt <- staticCalInterval(args(1), "interval");
                   zId <- zoneId(args(2)))
                yield
                  sparkexpr.TimeAdd(
                    expr,
                    sparkexpr.Literal(
                      calInt,
                      CalendarIntervalType),
                    Some(zId.toString))
            case id if id.symbol == dateAddIntervalSym =>
              for (expr <- CatalystExpression.unapply(args(0));
                   calInt <- staticCalInterval(args(1), "interval")
                   ) yield sparkexpr.DateAddInterval(expr,
                sparkexpr.Literal(calInt, CalendarIntervalType))
            case id if id.symbol == monthsBetweenSym =>
              for (micros1 <- CatalystExpression.unapply(args(0));
                   micros2 <- CatalystExpression.unapply(args(1));
              roundoff <- CatalystExpression.unapply(args(2));
                   zId <- zoneId(args(3))
                   ) yield sparkexpr.MonthsBetween(micros1, micros2, roundoff, Some(zId.toString))
            case id if id.symbol == getNextDateForDayOfWeekSym =>
              for (expr <- CatalystExpression.unapply(args(0));
                   dayOfWeek <- CatalystExpression.unapply(args(1))
                   ) yield sparkexpr.NextDay(expr, dayOfWeek)
            case id if id.symbol == getLastDayOfMonth =>
              for (expr <- CatalystExpression.unapply(args(0))
                   ) yield sparkexpr.LastDay(expr)
            case id if id.symbol == truncDateSym =>
              for (days <- CatalystExpression.unapply(args(0));
                   level <- CatalystExpression.unapply(args(1))) yield
                sparkexpr.TruncDate(days, level)
            case id if id.symbol == truncTimestampSym =>
              for (micros <- CatalystExpression.unapply(args(0));
                   level <- CatalystExpression.unapply(args(1));
                   zId <- zoneId(args(2))
                   ) yield
                sparkexpr.TruncTimestamp(level, micros, Some(zId.toString))
            case id if id.symbol == fromUTCTimeSym =>
              for (expr <- CatalystExpression.unapply(args(0));
                   zId <- CatalystExpression.unapply(args(1))) yield
                sparkexpr.FromUTCTimestamp(expr, zId)
            case id if id.symbol == toUTCTimeSym =>
              for (expr <- CatalystExpression.unapply(args(0));
                   zId <- CatalystExpression.unapply(args(1))) yield
                sparkexpr.ToUTCTimestamp(expr, zId)
            case id if id.symbol == currentTimestampSym =>
              Some(sparkexpr.CurrentTimestamp())
            case id if id.symbol == currentDateSym =>
              for (zId <- zoneId(args(0))) yield
                sparkexpr.CurrentDate(Some(zId.toString))
            case _ => None
          }
        case _ => None
      }
  }

  private def zoneId(t: mTree): Option[ZoneId] =
    staticValue[ZoneId](t, "evaluate ZoneId expression as a static value")

  private def staticIntValue(t: mTree, typStr: String): Option[Int] =
    staticValue[Int](t, s"evaluate ${typStr} expression as a static int value")

  private def staticLongValue(t: mTree, typStr: String): Option[Long] =
    staticValue[Long](t, s"evaluate ${typStr} expression as a static long value")

  private def staticCalInterval(t: mTree, typStr: String): Option[CalendarInterval] =
    staticValue[CalendarInterval](t,
      s"evaluate ${typStr} expression as a static cal_interval value"
    )

}
