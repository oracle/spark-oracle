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

import org.apache.spark.sql.oracle.AbstractTest
import org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder

class MacrosAbstractTest extends AbstractTest {

  import scala.tools.reflect.ToolBox
  import org.apache.spark.sql.catalyst.ScalaReflection._
  import universe._

  protected val tb = mirror.mkToolBox()

  protected def eval(fnTree : Tree) : Either[_, SQLMacroExpressionBuilder] = {
    tb.eval(
      q"""{
          new org.apache.spark.sql.defineMacros.SparkSessionMacroExt(
             org.apache.spark.sql.hive.test.oracle.TestOracleHive.sparkSession
             ).udm(${fnTree})
          }
        """).asInstanceOf[Either[_, SQLMacroExpressionBuilder]]
  }

  protected def register(nm : String, fnTree : Tree): Unit = {
    tb.eval(
      q"""{
          import org.apache.spark.sql.defineMacros._
          val ss = org.apache.spark.sql.hive.test.oracle.TestOracleHive.sparkSession
          ss.registerMacro($nm,ss.udm(${fnTree}))
        }"""
    )
  }

  // scalastyle:off println
  protected def handleMacroOutput(r: Either[Any, SQLMacroExpressionBuilder]) = {
    r match {
      case Left(fn) => println(s"Failed to create expression for ${fn}")
      case Right(fb) =>
        val s = fb.macroExpr.treeString(false).split("\n").
          map(s => if (s.length > 100) s.substring(0, 97) + "..." else s).mkString("\n")
        println(
          s"""Spark SQL expression is
             |${fb.macroExpr.sql}""".stripMargin)
    }
  }

  def printOut(s : => String) : Unit = {
    println(s)
  }

  // scalastyle:on

}
