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

import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, LeafExpression, Unevaluable}
import org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder.deserialize
import org.apache.spark.sql.types.DataType


/**
 * A [[org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder]]
 * for ''Spark SQL macros''
 *
 * @param macroExpr
 */
case class SQLMacroExpressionBuilder(macroExprSer : Array[Byte])
  extends Function1[Seq[Expression], Expression] {

  @transient lazy val macroExpr = deserialize(macroExprSer)

  override def apply(args: Seq[Expression]): Expression = {
    macroExpr transformUp {
      case MacroArg(argPos, dt) if argPos < args.size &&
        Cast.canCast(args(argPos).dataType, dt) =>
        if (dt == args(argPos).dataType) {
          args(argPos)
        } else {
          Cast(args(argPos), dt)
        }
    }
  }
}

/**
 * Represent holes in ''Macro SQL'' that will be filled in with the macro invocation argument
 * expressions.
 *
 * @param argPos
 * @param dataType
 */
@SerialVersionUID(-4890323739479048322L)
case class MacroArg(argPos : Int,
                    dataType : DataType)
  extends LeafExpression with Unevaluable {

  override def nullable: Boolean = true

  override def sql: String = s"macroarg($argPos)"

}

object SQLMacroExpressionBuilder {


  /**
   * This is not ideal. On each macro invocation we are setting up a [[JavaSerializer]].
   * This is needed because the macro invocation runs in an independent ClassLoader.
   *
   * The alternate to this is to implement [[Liftable]] for all Expression classes.
   * This is a lot of work. Deferring this for now.
   *
   * The `deserialize` call happens within an `SparkEnv`; but we don't know
   * which [[Serializer]] is configured within it; so we use our own
   * [[JavaSerializer]] to deserialize the ''macroExprSer''.
   *
   */
  def serializerInstance : SerializerInstance = {
    val sC = new SparkConf(false)
    val factory = new JavaSerializer(sC)
    factory.newInstance()
  }

  def serialize(e : Expression) : Array[Byte] = {
    val bb = serializerInstance.serialize[Expression](e)
    if (bb.hasArray) {
      bb.array()
    } else {
      val arr = new Array[Byte](bb.remaining())
      bb.get(arr)
      arr
    }
  }

  def deserialize(arr : Array[Byte]) : Expression = {
    val bb = ByteBuffer.wrap(arr)
    serializerInstance.deserialize[Expression](bb)
  }
}