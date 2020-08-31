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

import org.apache.spark.sql.catalyst.{expressions => sparkexpr}
import org.apache.spark.sql.types.{DataType, StructType}

trait Structs { self: ExprTranslator =>

  import macroUniverse._

  private object VarAndFieldName {
    def unapply(t : mTree) :
    Option[(mTermName, String)] = t match {
      case Select(Ident(varNm), TermName(fNm)) if varNm.isTermName =>
        Some((varNm.toTermName, fNm))
      case _ => None
    }
  }

  private def fieldIndex(dt : DataType,
                         fNm : String) : Option[Int] = {
    dt match {
      case sT : StructType => sT.getFieldIndex(fNm)
      case _ => None
    }
  }

  object FieldAccess {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      t match {
        case VarAndFieldName(varNm, fNm) =>
          for (
            vInfo <- scope.get(varNm);
          fIdx <- fieldIndex(vInfo.typInfo.catalystType, fNm)
          ) yield sparkexpr.GetStructField(vInfo.rhsExpr, fIdx, Some(fNm))
        case q"$l.$r" =>
          for (
          lExpr <- CatalystExpression.unapply(l);
          fNm = r.decodedName.toString;
          fIdx <- fieldIndex(lExpr.dataType, fNm)
          ) yield sparkexpr.GetStructField(lExpr, fIdx, Some(fNm))
        case _ => None
      }
  }

  object StructConstruct {

    private def isADTConstruction(applyTree : mTree) : Boolean = {
      applyTree.tpe != null &&
        applyTree.symbol == applyTree.tpe.resultType.companion.member(TermName("apply"))
    }

    private def isCandidateType(dt : DataType,
                                numArgs : Int) : Boolean = {

      dt match {
        case sT: StructType if sT.fields.size == numArgs => true
        case _ => false
      }
    }

    private def isCandidateParms(params : Seq[sparkexpr.Expression],
                         sT : StructType) : Boolean = {
      params.zip(sT.fields.map(_.dataType)).forall {
        case (e, dt) => sparkexpr.Cast.canCast(e.dataType, dt)
      }
    }

    def unapply(t: mTree): Option[sparkexpr.Expression] =
      t match {
        case q"$id(..$args)" if isADTConstruction(id) =>
          for (
          typInfo <- TypeInfo.unapply(t) if isCandidateType(typInfo.catalystType, args.size);
          sT = typInfo.catalystType.asInstanceOf[StructType];
          exprs <- CatalystExpressions.unapplySeq(args) if isCandidateParms(exprs, sT)
          ) yield {
            val params = exprs.zip(sT.fieldNames).flatMap {
              case (e, fN) => Seq(sparkexpr.Literal(fN), e)
            }
            sparkexpr.CreateNamedStruct(params)
          }
        case _ => None
      }
  }
}
