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

import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.functions._
import org.apache.spark.sql.sqlmacros.{SQLMacro, _}

// scalastyle:off
object defineMacros {
// scalastyle:on

  // scalastyle:off line.size.limit

  class SparkSessionMacroExt(val sparkSession: SparkSession) extends AnyVal {

    def udm[RT, A1](f: Function1[A1, RT]) :
    Either[Function1[A1, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm1_impl[RT, A1]

    def registerMacro[RT : TypeTag, A1 : TypeTag](nm : String,
                                                  udm : Either[Function1[A1, RT], SQLMacroExpressionBuilder]
                                                 ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }

    def udm[RT, A1, A2](f: Function2[A1, A2, RT]) :
    Either[Function2[A1, A2, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm2_impl[RT, A1, A2]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag](nm : String,
                                                                udm : Either[Function2[A1, A2, RT], SQLMacroExpressionBuilder]
                                                               ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }

    // GENERATED using [[GenMacroFuncs]

    def udm[RT, A1, A2, A3](f: Function3[A1, A2, A3, RT]) :
    Either[Function3[A1, A2, A3, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm3_impl[RT, A1, A2, A3]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag, A3 : TypeTag](nm : String,
                                                                              udm : Either[Function3[A1, A2, A3, RT], SQLMacroExpressionBuilder]
                                                                             ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }


    def udm[RT, A1, A2, A3, A4](f: Function4[A1, A2, A3, A4, RT]) :
    Either[Function4[A1, A2, A3, A4, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm4_impl[RT, A1, A2, A3, A4]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag](nm : String,
                                                                                            udm : Either[Function4[A1, A2, A3, A4, RT], SQLMacroExpressionBuilder]
                                                                                           ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }


    def udm[RT, A1, A2, A3, A4, A5](f: Function5[A1, A2, A3, A4, A5, RT]) :
    Either[Function5[A1, A2, A3, A4, A5, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm5_impl[RT, A1, A2, A3, A4, A5]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag](nm : String,
                                                                                                          udm : Either[Function5[A1, A2, A3, A4, A5, RT], SQLMacroExpressionBuilder]
                                                                                                         ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }


    def udm[RT, A1, A2, A3, A4, A5, A6](f: Function6[A1, A2, A3, A4, A5, A6, RT]) :
    Either[Function6[A1, A2, A3, A4, A5, A6, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm6_impl[RT, A1, A2, A3, A4, A5, A6]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag](nm : String,
                                                                                                                        udm : Either[Function6[A1, A2, A3, A4, A5, A6, RT], SQLMacroExpressionBuilder]
                                                                                                                       ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }


    def udm[RT, A1, A2, A3, A4, A5, A6, A7](f: Function7[A1, A2, A3, A4, A5, A6, A7, RT]) :
    Either[Function7[A1, A2, A3, A4, A5, A6, A7, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm7_impl[RT, A1, A2, A3, A4, A5, A6, A7]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag, A7 : TypeTag](nm : String,
                                                                                                                                      udm : Either[Function7[A1, A2, A3, A4, A5, A6, A7, RT], SQLMacroExpressionBuilder]
                                                                                                                                     ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }


    def udm[RT, A1, A2, A3, A4, A5, A6, A7, A8](f: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]) :
    Either[Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm8_impl[RT, A1, A2, A3, A4, A5, A6, A7, A8]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag, A7 : TypeTag, A8 : TypeTag](nm : String,
                                                                                                                                                    udm : Either[Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT], SQLMacroExpressionBuilder]
                                                                                                                                                   ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }


    def udm[RT, A1, A2, A3, A4, A5, A6, A7, A8, A9](f: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]) :
    Either[Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm9_impl[RT, A1, A2, A3, A4, A5, A6, A7, A8, A9]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag, A7 : TypeTag, A8 : TypeTag, A9 : TypeTag](nm : String,
                                                                                                                                                                  udm : Either[Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT], SQLMacroExpressionBuilder]
                                                                                                                                                                 ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }


    def udm[RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10](f: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]) :
    Either[Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm10_impl[RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10]

    def registerMacro[RT : TypeTag, A1 : TypeTag, A2 : TypeTag, A3 : TypeTag, A4 : TypeTag, A5 : TypeTag, A6 : TypeTag, A7 : TypeTag, A8 : TypeTag, A9 : TypeTag, A10 : TypeTag](nm : String,
                                                                                                                                                                                 udm : Either[Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT], SQLMacroExpressionBuilder]
                                                                                                                                                                                ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }
  }

  implicit def ssWithMacros(ss : SparkSession) : SparkSessionMacroExt = new SparkSessionMacroExt(ss)

}
