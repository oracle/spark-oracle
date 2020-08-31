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

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.{expressions => sparkexpr}

// scalastyle:off
/**
 * A marker interface for code inside Macros, that provides a way
 * for users to write scala code on
 * [[sparkexpr.Expression ] catalyst spark expressions].
 */
object StringUtils {

  sealed trait ConcatWSArgType
  case class StringConcatWARg(s : String) extends ConcatWSArgType
  case class ArrStringConcatWARg(arr : Array[String]) extends ConcatWSArgType
  implicit def toConcatWSStr(s : String) : ConcatWSArgType = StringConcatWARg(s)
  implicit def toConcatWSArrStr(arr : Array[String]) : ConcatWSArgType = ArrStringConcatWARg(arr)

  def concatWs(sep : String, inputs : ConcatWSArgType*) : String = ???

  def elt(n : Int, inputs : String*) : String = ???
  def elt(n : Int, inputs : Array[Byte]*) : Array[Byte] = ???

  def overlay(input : String, replace : String, pos : Int) : String = ???
  def overlay(input : String, replace : String, pos : Int, len : Int) : String = ???
  def overlay(input : Array[Byte], replace : Array[Byte], pos : Int) : String = ???
  def overlay(input : Array[Byte], replace : Array[Byte], pos : Int, len : Int) : String = ???

  def translate(input : String, from : String, to : String) : String = ???
}
