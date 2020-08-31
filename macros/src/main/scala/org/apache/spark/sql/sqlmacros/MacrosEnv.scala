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

import scala.reflect.macros.blackbox
import scala.tools.reflect.ToolBox

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

trait MacrosEnv {

  val c: blackbox.Context

  val macroUniverse: c.universe.type = c.universe
  val runtimeUniverse = MacrosScalaReflection.universe
  type mType = macroUniverse.Type
  type mTree = macroUniverse.Tree
  type ruTree = runtimeUniverse.Tree
  type ruType = runtimeUniverse.Type
  type mTermName = macroUniverse.TermName

  lazy val ruToolBox = MacrosScalaReflection.mirror.mkToolBox()

  lazy val ruImporter = {
    val importer0 = runtimeUniverse.internal.createImporter(macroUniverse)
    importer0.asInstanceOf[runtimeUniverse.Importer {val from: macroUniverse.type}]
  }

  private[sqlmacros] def _expressionEncoder(rTyp : ruType) : ExpressionEncoder[_] = {
    MacroUtils.expressionEncoder(rTyp)
  }

  def convertTree(tree : mTree) : ruTree = {
    val imported = ruImporter.importTree(tree)
    val treeR = ruToolBox.untypecheck(imported.duplicate)
    ruToolBox.typecheck(treeR, ruToolBox.TERMmode)
  }

  def convertType(typ : mType) : ruType = {
    ruImporter.importType(typ)
  }

  def eval_tree(tree : mTree) : Any = {
    val e = c.Expr(c.untypecheck(tree.duplicate))
    c.eval(e)
  }
}

object MacroUtils {

  import MacrosScalaReflection._

  val ru = universe

  def expressionEncoder[T : ru.TypeTag](): ExpressionEncoder[T] = {
    import scala.reflect.ClassTag

    val tpe = ru.typeTag[T].in(mirror).tpe

    val cls = mirror.runtimeClass(tpe)
    val serializer = serializerForType(tpe)
    val deserializer = deserializerForType(tpe)

    new ExpressionEncoder[T](
      serializer,
      deserializer,
      ClassTag[T](cls))
  }

  def expressionEncoder(tpe : ru.Type) : ExpressionEncoder[_] = {
    import scala.reflect.ClassTag

    val cls = mirror.runtimeClass(tpe)
    val serializer = serializerForType(tpe)
    val deserializer = deserializerForType(tpe)

    new ExpressionEncoder(
      serializer,
      deserializer,
      ClassTag(cls))
  }
}
