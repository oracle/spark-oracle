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

import java.sql.{Date, Timestamp}
import java.time.{DateTimeException, Instant, LocalDate, ZoneId}

import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

// scalastyle:off
/**
 * A marker interface for code inside Macros, that is a replacement for functions in
 * [[org.apache.spark.sql.catalyst.util.DateTimeUtils]]. Use the
 * functions with [[Date]] values instead of [[Int]], [[Timestamp]]
 * values instead of [[Long]] and [[String]] values instead of
 * [[org.apache.spark.unsafe.types.UTF8String]] values. Other interface changes:
 * - getNextDateForDayOfWeek take String values for the dayOfWeek param
 * - `truncDate`, `truncTimestamp` take String values for the level param
 */
object DateTimeUtils {

  /**
   * Converts days since 1970-01-01 at the given zone ID to microseconds since 1970-01-01 00:00:00Z.
   */
  def daysToMicros(days: Date, zoneId: ZoneId): Timestamp = ???

  /**
   * Trims and parses a given UTF8 timestamp string to the corresponding a corresponding [[Long]]
   * value. The return type is [[Option]] in order to distinguish between 0L and null. The following
   * formats are allowed:
   *
   * `yyyy`
   * `yyyy-[m]m`
   * `yyyy-[m]m-[d]d`
   * `yyyy-[m]m-[d]d `
   * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   *
   * where `zone_id` should have one of the forms:
   *   - Z - Zulu time zone UTC+0
   *   - +|-[h]h:[m]m
   *   - A short id, see https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#SHORT_IDS
   *   - An id with one of the prefixes UTC+, UTC-, GMT+, GMT-, UT+ or UT-,
   *     and a suffix in the formats:
   *     - +|-h[h]
   *     - +|-hh[:]mm
   *     - +|-hh:mm:ss
   *     - +|-hhmmss
   *  - Region-based zone IDs in the form `area/city`, such as `Europe/Paris`
   */
  def stringToTimestamp(s: String, timeZoneId: ZoneId): Option[Timestamp] = ???

  def stringToTimestampAnsi(s: String, timeZoneId: ZoneId): Timestamp = ???

  /**
   * Gets the number of microseconds since the epoch of 1970-01-01 00:00:00Z from the given
   * instance of `java.time.Instant`. The epoch microsecond count is a simple incrementing count of
   * microseconds where microsecond 0 is 1970-01-01 00:00:00Z.
   */
  def instantToMicros(instant: Instant): Timestamp = ???

  /**
   * Converts the local date to the number of days since 1970-01-01.
   */
  def localDateToDays(localDate: LocalDate): Date = ???

  /**
   * Trims and parses a given UTF8 date string to a corresponding [[Int]] value.
   * The return type is [[Option]] in order to distinguish between 0 and null. The following
   * formats are allowed:
   *
   * `yyyy`
   * `yyyy-[m]m`
   * `yyyy-[m]m-[d]d`
   * `yyyy-[m]m-[d]d `
   * `yyyy-[m]m-[d]d *`
   * `yyyy-[m]m-[d]dT*`
   */
  def stringToDate(s: String, zoneId: ZoneId): Option[Date] = ???

  /**
   * Returns the hour value of a given timestamp value. The timestamp is expressed in microseconds.
   */
  def getHours(micros: Timestamp, zoneId: ZoneId): Int = ???

  /**
   * Returns the minute value of a given timestamp value. The timestamp is expressed in
   * microseconds since the epoch.
   */
  def getMinutes(micros: Timestamp, zoneId: ZoneId): Int = ???

  /**
   * Returns the second value of a given timestamp value. The timestamp is expressed in
   * microseconds since the epoch.
   */
  def getSeconds(micros: Timestamp, zoneId: ZoneId): Int = ???

  /**
   * Returns the seconds part and its fractional part with microseconds.
   */
  def getSecondsWithFraction(micros: Timestamp, zoneId: ZoneId): Decimal = ???

  /**
   * Returns local seconds, including fractional parts, multiplied by 1000000.
   *
   * @param micros The number of microseconds since the epoch.
   * @param zoneId The time zone id which milliseconds should be obtained in.
   */
  def getMicroseconds(micros: Timestamp, zoneId: ZoneId): Int = ???

  /**
   * Returns the 'day in year' value for the given number of days since 1970-01-01.
   */
  def getDayInYear(days: Date): Int = ???

  /**
   * Returns the year value for the given number of days since 1970-01-01.
   */
  def getYear(days: Date): Int = ???

  /**
   * Returns the year which conforms to ISO 8601. Each ISO 8601 week-numbering
   * year begins with the Monday of the week containing the 4th of January.
   */
  def getWeekBasedYear(days: Date): Int = ???

  /** Returns the quarter for the given number of days since 1970-01-01. */
  def getQuarter(days: Date): Int = ???

  /**
   * Returns the month value for the given number of days since 1970-01-01.
   * January is month 1.
   */
  def getMonth(days: Date): Int = ???

  /**
   * Returns the 'day of month' value for the given number of days since 1970-01-01.
   */
  def getDayOfMonth(days: Date): Int = ???

  /**
   * Returns the day of the week for the given number of days since 1970-01-01
   * (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
   */
  def getDayOfWeek(days: Date): Int = ???

  /**
   * Returns the day of the week for the given number of days since 1970-01-01
   * (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
   */
  def getWeekDay(days: Date): Int = ???

  /**
   * Returns the week of the year of the given date expressed as the number of days from 1970-01-01.
   * A week is considered to start on a Monday and week 1 is the first week with > 3 days.
   */
  def getWeekOfYear(days: Date): Int = ???

  /**
   * Adds an year-month interval to a date represented as days since 1970-01-01.
   * @return a date value, expressed in days since 1970-01-01.
   */
  def dateAddMonths(days: Date, months: Int): Date = ???

  /**
   * Adds a full interval (months, days, microseconds) a timestamp represented as the number of
   * microseconds since 1970-01-01 00:00:00Z.
   * @return A timestamp value, expressed in microseconds since 1970-01-01 00:00:00Z.
   */
  def timestampAddInterval(start: Timestamp, int: CalendarInterval, zoneId: ZoneId): Timestamp =
    ???

  /**
   * Adds the interval's months and days to a date expressed as days since the epoch.
   * @return A date value, expressed in days since 1970-01-01.
   *
   * @throws DateTimeException if the result exceeds the supported date range
   * @throws IllegalArgumentException if the interval has `microseconds` part
   */
  def dateAddInterval(start: Date, interval: CalendarInterval): Date = ???

  /**
   * Returns number of months between micros1 and micros2. micros1 and micros2 are expressed in
   * microseconds since 1970-01-01. If micros1 is later than micros2, the result is positive.
   *
   * If micros1 and micros2 are on the same day of month, or both are the last day of month,
   * returns, time of day will be ignored.
   *
   * Otherwise, the difference is calculated based on 31 days per month.
   * The result is rounded to 8 decimal places if `roundOff` is set to true.
   */
  def monthsBetween(
      micros1: Timestamp,
      micros2: Timestamp,
      roundOff: Boolean,
      zoneId: ZoneId): Double = ???

  /**
   * Returns the first date which is later than startDate and is of the given dayOfWeek.
   * dayOfWeek can be
   * "SU" | "SUN" | "SUNDAY", "MO" | "MON" | "MONDAY",
   * "TU" | "TUE" | "TUESDAY", "WE" | "WED" | "WEDNESDAY",
   * "TH" | "THU" | "THURSDAY" => THURSDAY, "FR" | "FRI" | "FRIDAY" => FRIDAY,
   * "SA" | "SAT" | "SATURDAY"
   */
  def getNextDateForDayOfWeek(startDay: Date, dayOfWeek: String): Date = ???

  /** Returns last day of the month for the given number of days since 1970-01-01. */
  def getLastDayOfMonth(days: Date): Date = ???

  /**
   * Returns the trunc date from original date and trunc level.
   * level can be:
   * "WEEK", "MON" | "MONTH" | "MM", "QUARTER",
   * "YEAR" | "YYYY" | "YY"
   */
  def truncDate(days: Date, level: String): Date = ???

  /**
   * Returns the trunc date time from original date time and trunc level.
   * level can be:
   * "MICROSECOND", "MILLISECOND", "SECOND", "MINUTE", "HOUR",
   * "DAY" | "DD", "WEEK", "MON" | "MONTH" | "MM", "QUARTER",
   * "YEAR" | "YYYY" | "YY"
   */
  def truncTimestamp(micros: Timestamp, level: String, zoneId: ZoneId): Timestamp = ???

  /**
   * Returns a timestamp of given timezone from UTC timestamp, with the same string
   * representation in their timezone.
   */
  def fromUTCTime(micros: Timestamp, timeZone: String): Timestamp = ???

  /**
   * Returns a utc timestamp from a given timestamp from a given timezone, with the same
   * string representation in their timezone.
   */
  def toUTCTime(micros: Timestamp, timeZone: String): Timestamp = ???

  /**
   * Obtains the current instant as microseconds since the epoch at the UTC time zone.
   */
  def currentTimestamp(): Timestamp = ???

  /**
   * Obtains the current date as days since the epoch in the specified time-zone.
   */
  def currentDate(zoneId: ZoneId): Date = ???

  /**
   * Converts notational shorthands that are converted to ordinary timestamps.
   *
   * @param input A trimmed string
   * @param zoneId Zone identifier used to get the current date.
   * @return Some of microseconds since the epoch if the conversion completed
   *         successfully otherwise None.
   */
  def convertSpecialTimestamp(input: String, zoneId: ZoneId): Option[Timestamp] = ???

  /**
   * Converts notational shorthands that are converted to ordinary dates.
   *
   * @param input A trimmed string
   * @param zoneId Zone identifier used to get the current date.
   * @return Some of days since the epoch if the conversion completed successfully otherwise None.
   */
  def convertSpecialDate(input: String, zoneId: ZoneId): Option[Date] = ???

  /**
   * Subtracts two dates expressed as days since 1970-01-01.
   *
   * @param endDay The end date, exclusive
   * @param startDay The start date, inclusive
   * @return An interval between two dates. The interval can be negative
   *         if the end date is before the start date.
   */
  def subtractDates(endDay: Date, startDay: Date): CalendarInterval = ???

}
