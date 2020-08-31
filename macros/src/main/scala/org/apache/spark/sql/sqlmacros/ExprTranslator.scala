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

import scala.collection.mutable.{Map => MMap}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions => sparkexpr}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.DataType

trait ExprTranslator extends MacrosEnv with ExprBuilders with ExprOptimize with Logging {
  import macroUniverse._

  object MacroTransException extends Exception

  def sparkSession : Option[SparkSession]

  val scope = MMap[mTermName, ValInfo]()

  /**
   * - log the issue and also record it in the context as a warning.
   * - don't issue a `c.abort` or `c.error` because it doesn't let us recover and return a `None`.
   *   - not doing a `c.error` or `c.abort` is safe because we immediately throw and go into
   *     'exit mode'(ie to do any cleanup and return a None)
   *
   * @param tree
   * @param reason
   * @param kind
   */
  def logIssue(tree : mTree,
               reason : String,
               kind : String) : Unit = {
    val msg = s"$reason: '${showCode(tree)}}'"
    logDebug(
      s"""Spark SQL Macro translation ${kind} at pos ${tree.pos.toString}:
         |msg""".stripMargin)
    c.warning(tree.pos, msg)
  }

  def quit(tree : mTree,
           reason : String) : Nothing = {
    logIssue(tree, reason, "error")
    throw MacroTransException
  }

  def warn(tree : mTree, reason : String) : Unit = logIssue(tree, reason, "warning")

  def doWithWarning[T](tree : mTree,
                    action : String,
                    fn : => T) : Option[T] = {
    try {
      Some(fn)
    } catch {
      case e : Exception =>
      warn(tree, s"Failed to do $action(exception = ${e.getMessage})")
      None
      case e : Error =>
        warn(tree, s"Failed to do $action(error = ${e.getMessage})")
        None
    }
  }

  def staticValue[T : TypeTag](tree : mTree, action : String) : Option[T] = {
    if (tree.tpe <:< typeOf[T]) {
      doWithWarning[T](tree, "evaluate ZoneId expression a static value", {
        val v = eval_tree(tree)
        v.asInstanceOf[T]
      })
    } else None
  }

  /**
   * There should be nothing to do here.
   * For functions the [[ExpressionEncoder]] for the argument's [[DataType]]
   * will convert an internal value into a catalyst value. In case of macros
   * the catalyst value is fed to a catalyst Expression that is equivalent to
   * the function body.
   * @param typInfo
   * @param pos
   * @return
   */
  private def paramSparkExpr(typInfo : TypeInfo,
                             pos : Int) : sparkexpr.Expression = {
    MacroArg(pos, typInfo.catalystType)
  }

  def translateExprTree(tree : mTree) : sparkexpr.Expression = tree match {
    case CatalystExpression(e) => e
    case _ => quit(tree, "Not able to translate to a Spark Expression")
  }

  def translateValDef(tree : ValDef,
                     exprBldr : Function2[TypeInfo, mTree, sparkexpr.Expression]
                    ) : Unit = (tree, exprBldr) match {
    case ValInfo(_) => ()
    case _ => quit(tree, "Not able to translate Value definition")
  }

  def translateParam(pos : Int, tree : mTree) : Unit = tree match {
    case v : ValDef => translateValDef(v, (typInfo, tree) => paramSparkExpr(typInfo, pos))
    case _ => quit(tree, "Not able to translate function param")
  }


  def translateStat(tree : mTree) : Unit = tree match {
    case v : ValDef => translateValDef(v, (typInfo, tree) => translateExprTree(tree))
    case _ => warn(tree, s"Ignoring statement")
  }


  def expressionEncoder(typ : mType) : ExpressionEncoder[_] = {
    _expressionEncoder(convertType(typ))
  }

  def extractFuncParamsStats(fTree : mTree) : (Seq[mTree], Seq[mTree]) = fTree match {
      case q"(..$params) => {..$stats}" => (params, stats)
      case q"{(..$params) => {..$stats}}" => (params, stats)
      case _ => quit(fTree, "Not able to recognize function structure")
    }

  case class TypeInfo(mTyp : mType,
                      rTyp : ruType,
                      catalystType : DataType,
                      _exprEnc : () => ExpressionEncoder[_]) {
    lazy val exprEnc = _exprEnc()
    override def toString: String = s"""${catalystType.toString}"""
  }
  object TypeInfo {
    def unapply(typTree : mTree) : Option[TypeInfo] = unapply(typTree.tpe)

    def unapply(typ : mType) : Option[TypeInfo] = (scala.util.Try {
      val rTyp = convertType(typ)
      val cSchema = MacrosScalaReflection.schemaFor(rTyp)
      TypeInfo(typ, rTyp, cSchema.dataType, () => _expressionEncoder(rTyp))
    }).toOption
  }

  case class ValInfo(vDef : macroUniverse.ValDef,
                     name : String,
                     typInfo : TypeInfo,
                     rhsExpr : sparkexpr.Expression) {
    override def toString: String = {
      s"""ValDef:
         |  vDef: ${macroUniverse.show(vDef)}
         |  name: ${name}
         |  type: ${typInfo.toString}""".stripMargin
    }
  }

  object ValInfo {
    def unapply(arg : (mTree, Function2[TypeInfo, mTree, sparkexpr.Expression])) : Option[ValInfo]
    = {
      val (t, exprBldr) = arg
      t match {
        case vDef@ValDef(mods, tNm, TypeInfo(tInfo), rhsTree) =>
          val nm = tNm.decodedName.toString
          val vInfo = ValInfo(vDef, nm, tInfo, exprBldr(tInfo, rhsTree))
          scope(tNm) = vInfo
          Some(vInfo)
        case _ => None
      }
    }
  }

  /**
   * return the elem being operated on and args1 and args2
   */
  object InstanceMethodCall {
    def unapply(t: mTree): Option[(mTree, Seq[mTree], Seq[mTree])] = t match {
      case q"$ent.$_" => Some((ent, Seq.empty, Seq.empty))
      case q"$ent.$_[..$_]" => Some((ent, Seq.empty, Seq.empty))
      case q"$ent.$_(..$args1)" => Some((ent, args1, Seq.empty))
      case q"$ent.$_[..$_](..$args1)" => Some((ent, args1, Seq.empty))
      case q"$ent.$_(..$args1)(..$args2)" => Some((ent, args1, args2))
      case q"$ent.$_[..$_](..$args1)(..$args2)" => Some((ent, args1, args2))
      case _ => None
    }
  }

  object ModuleMethodCall {
    def unapply(t: mTree): Option[(mTree, Seq[mTree], Seq[mTree])] = t match {
      case q"$id(..$args1)" => Some((id, args1, Seq.empty))
      case q"$id[..$_](..$args1)" => Some((id, args1, Seq.empty))
      case q"$id(..$args1)(..$args2)" => Some((id, args1, args2))
      case q"$id[..$_](..$args1)(..$args2)" => Some((id, args1, args2))
      case _ => None
    }
  }
}
