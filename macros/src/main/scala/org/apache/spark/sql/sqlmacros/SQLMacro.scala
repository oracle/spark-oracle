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

import scala.language.experimental.macros
import scala.reflect.macros.blackbox._

import org.apache.spark.sql.catalyst.{expressions => sparkexpr}
import org.apache.spark.sql.oracle.OraSparkUtils

// scalastyle:off line.size.limit
class SQLMacro(val c : Context) extends ExprTranslator {

  lazy val sparkSession = OraSparkUtils.currentSparkSessionOption

  def buildExpression(params : Seq[mTree],
                      stats : Seq[mTree]) : Option[sparkexpr.Expression] = {

    try {

      for ((p, i) <- params.zipWithIndex) {
        translateParam(i, p)
      }

      for (s <- stats.init) {
        translateStat(s)
      }

      Some(translateExprTree(stats.last)).map(optimizeExpr)

    } catch {
      case MacroTransException => None
      case e : Throwable => throw e
    }

  }

  def translateFunc(fTree : mTree) : Option[sparkexpr.Expression] = {
    val (params, stats) = extractFuncParamsStats(fTree)
    buildExpression(params, stats)
  }

  def udm1_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag](f : c.Expr[Function1[A1, RT]])
  : c.Expr[Either[Function1[A1, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function1[A1, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function1[A1, RT], SQLMacroExpressionBuilder]](
        q"scala.util.Left(${f.tree})")
    }
  }

  // GENERATED using [[GenMacroFuncs]

  def udm2_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag](f : c.Expr[Function2[A1, A2, RT]])
  : c.Expr[Either[Function2[A1, A2, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function2[A1, A2, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function2[A1, A2, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }

  def udm3_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag, A3 : c.WeakTypeTag](f : c.Expr[Function3[A1, A2, A3, RT]])
  : c.Expr[Either[Function3[A1, A2, A3, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function3[A1, A2, A3, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function3[A1, A2, A3, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }

  def udm4_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag, A3 : c.WeakTypeTag, A4 : c.WeakTypeTag](f : c.Expr[Function4[A1, A2, A3, A4, RT]])
  : c.Expr[Either[Function4[A1, A2, A3, A4, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function4[A1, A2, A3, A4, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function4[A1, A2, A3, A4, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }

  def udm5_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag, A3 : c.WeakTypeTag, A4 : c.WeakTypeTag, A5 : c.WeakTypeTag](f : c.Expr[Function5[A1, A2, A3, A4, A5, RT]])
  : c.Expr[Either[Function5[A1, A2, A3, A4, A5, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function5[A1, A2, A3, A4, A5, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function5[A1, A2, A3, A4, A5, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }

  def udm6_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag, A3 : c.WeakTypeTag, A4 : c.WeakTypeTag, A5 : c.WeakTypeTag, A6 : c.WeakTypeTag](f : c.Expr[Function6[A1, A2, A3, A4, A5, A6, RT]])
  : c.Expr[Either[Function6[A1, A2, A3, A4, A5, A6, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function6[A1, A2, A3, A4, A5, A6, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function6[A1, A2, A3, A4, A5, A6, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }

  def udm7_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag, A3 : c.WeakTypeTag, A4 : c.WeakTypeTag, A5 : c.WeakTypeTag, A6 : c.WeakTypeTag, A7 : c.WeakTypeTag](f : c.Expr[Function7[A1, A2, A3, A4, A5, A6, A7, RT]])
  : c.Expr[Either[Function7[A1, A2, A3, A4, A5, A6, A7, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function7[A1, A2, A3, A4, A5, A6, A7, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function7[A1, A2, A3, A4, A5, A6, A7, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }

  def udm8_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag, A3 : c.WeakTypeTag, A4 : c.WeakTypeTag, A5 : c.WeakTypeTag, A6 : c.WeakTypeTag, A7 : c.WeakTypeTag, A8 : c.WeakTypeTag](f : c.Expr[Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]])
  : c.Expr[Either[Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }

  def udm9_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag, A3 : c.WeakTypeTag, A4 : c.WeakTypeTag, A5 : c.WeakTypeTag, A6 : c.WeakTypeTag, A7 : c.WeakTypeTag, A8 : c.WeakTypeTag, A9 : c.WeakTypeTag](f : c.Expr[Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]])
  : c.Expr[Either[Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }

  def udm10_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag, A2 : c.WeakTypeTag, A3 : c.WeakTypeTag, A4 : c.WeakTypeTag, A5 : c.WeakTypeTag, A6 : c.WeakTypeTag, A7 : c.WeakTypeTag, A8 : c.WeakTypeTag, A9 : c.WeakTypeTag, A10 : c.WeakTypeTag](f : c.Expr[Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]])
  : c.Expr[Either[Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._
    val expr = translateFunc(f.tree)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Left(${f.tree})")
    }
  }
}

// scalastyle:off println
/* UNCOMMENT to generate macro signature functions
object GenMacroFuncs extends App {

  case class MacroCode(i : Int) {
    val argTypes : Seq[String] = (1 until i + 1).map(j => s"A$j")

    val funcType = {
      (argTypes :+ "RT").mkString(s"Function${i}[", ", ", "]")
    }

    def impl_method : String = {
      val methodNm = s"udm${i}_impl"

      val methodTypeArg = {
        val argTypeCtx : Seq[String] = argTypes.map(at => s"${at} : c.WeakTypeTag")
        ("RT : c.WeakTypeTag" +: argTypeCtx).mkString("[", ", ", "]")
      }

      val methodSig =
        s"""def ${methodNm}${methodTypeArg}(f : c.Expr[$funcType])
           |  : c.Expr[Either[${funcType}, SQLMacroExpressionBuilder]]""".stripMargin

      s"""$methodSig = {
         |
         |    import macroUniverse._
         |    val expr = translateFunc(f.tree)
         |
         |    expr match {
         |      case Some(e) =>
         |        val eSer = SQLMacroExpressionBuilder.serialize(e)
         |        c.Expr[Either[${funcType}, SQLMacroExpressionBuilder]](
         |          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder($${eSer}))")
         |      case None =>
         |        c.Expr[Either[${funcType}, SQLMacroExpressionBuilder]](
         |        q"scala.util.Left($${f.tree})")
         |    }
         |  }""".stripMargin
    }

    def udm_method : String = {
      val implmethodNm = s"udm${i}_impl"

      val methodTypeArg = ("RT" +: argTypes).mkString("[", ", ", "]")
      val methodSig = s"""def udm${methodTypeArg}(f: $funcType) :
         |    Either[$funcType, SQLMacroExpressionBuilder]""".stripMargin

      val regMacroTypeArg = {
        val argTypeCtx : Seq[String] = argTypes.map(at => s"${at} : TypeTag")
        ("RT : TypeTag" +: argTypeCtx).mkString("[", ", ", "]")
      }

      s"""$methodSig = macro SQLMacro.${implmethodNm}${methodTypeArg}
         |
         |def registerMacro${regMacroTypeArg}(nm : String,
         |                                  udm : Either[${funcType}, SQLMacroExpressionBuilder]
         |                                 ) : Unit = {
         |      udm match {
         |        case Left(fn) =>
         |          sparkSession.udf.register(nm, udf(fn))
         |        case Right(sqlMacroBldr) =>
         |          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
         |      }
         |}
         |""".stripMargin
    }

  }

  lazy val udm_impl_methods : Seq[String] = for (i <- (1 until 11)) yield  MacroCode(i).impl_method

  lazy val udm_methods : Seq[String] = for (i <- (1 until 11)) yield  MacroCode(i).udm_method


  // println(udm_impl_methods.mkString("\n\n"))

  println(udm_methods.mkString("\n\n"))

}
*/