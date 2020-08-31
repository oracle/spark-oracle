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
import org.apache.spark.sql.types.{ArrayType, MapType}

trait Collections {
  self: ExprBuilders with ExprTranslator =>

  import macroUniverse._

  object CollectionTrees {
    val arrObjTyp = typeOf[Array.type]
    val arrOpsTyp = typeOf[scala.collection.mutable.ArrayOps[_]]
    val mapObjTyp = typeOf[Map.type]
    val mapTyp = typeOf[Map[_, _]]

    def unapply(t: mTree): Option[sparkexpr.Expression] =
      Option(t match {
        case CollectionConstruct(e) => e
        case CollectionApply(e) => e
        case ArrayUnref(e) => e
        case ArrayUnwrap(e) => e
        case CollectionUtilsFunctions(e) => e
        case CollectionFunctions(e) => e
        case _ => null
      })

    object ArrayUnwrap {
      val wrapRefArraySym = predefTyp.member(TermName("wrapRefArray")).alternatives
      val wrapIntArraySym = predefTyp.member(TermName("wrapIntArray")).alternatives
      val wrapDoubleArraySym = predefTyp.member(TermName("wrapDoubleArray")).alternatives
      val wrapLongArraySym = predefTyp.member(TermName("wrapLongArray")).alternatives
      val wrapFloatArraySym = predefTyp.member(TermName("wrapFloatArray")).alternatives
      val wrapCharArraySym = predefTyp.member(TermName("wrapCharArray")).alternatives
      val wrapByteArraySym = predefTyp.member(TermName("wrapByteArray")).alternatives
      val wrapShortArraySym = predefTyp.member(TermName("wrapShortArray")).alternatives
      val wrapBooleanArraySym = predefTyp.member(TermName("wrapBooleanArray")).alternatives

      val wrapArraySyms = (wrapRefArraySym ++ wrapIntArraySym ++ wrapDoubleArraySym ++
        wrapLongArraySym ++ wrapFloatArraySym ++ wrapCharArraySym ++
        wrapByteArraySym ++ wrapShortArraySym ++ wrapBooleanArraySym).toSet

      def unapply(t: mTree): Option[sparkexpr.Expression] = t match {
        case ModuleMethodCall(id, args, args2) if wrapArraySyms.contains(t.symbol) =>
          CatalystExpression.unapply(args(0))
        case _ => None
      }
    }

    object ArrayUnref {
      val refArrayOpsSym = predefTyp.member(TermName("refArrayOps")).alternatives
      val intArrayOpsSym = predefTyp.member(TermName("intArrayOps")).alternatives
      val doubleArrayOpsSym = predefTyp.member(TermName("doubleArrayOps")).alternatives
      val longArrayOpsSym = predefTyp.member(TermName("longArrayOps")).alternatives
      val floatArrayOpsSym = predefTyp.member(TermName("floatArrayOps")).alternatives
      val charArrayOpsSym = predefTyp.member(TermName("charArrayOps")).alternatives
      val byteArrayOpsSym = predefTyp.member(TermName("byteArrayOps")).alternatives
      val shortArrayOpsSym = predefTyp.member(TermName("shortArrayOps")).alternatives
      val booleanArrayOpsSym = predefTyp.member(TermName("booleanArrayOps")).alternatives

      val arrayOpsSyms = (refArrayOpsSym ++ intArrayOpsSym ++ doubleArrayOpsSym ++
        longArrayOpsSym ++ floatArrayOpsSym ++ charArrayOpsSym ++
        byteArrayOpsSym ++ shortArrayOpsSym ++ booleanArrayOpsSym).toSet

      def unapply(t: mTree): Option[sparkexpr.Expression] = t match {
        case ModuleMethodCall(id, args, args2) if arrayOpsSyms.contains(t.symbol) =>
          CatalystExpression.unapply(args(0))
        case _ => None
      }
    }

    object CollectionFunctions {
      val arrSizeSym = arrOpsTyp.member(TermName("size")).alternatives
      val arrZipSym = arrOpsTyp.member(TermName("zip")).alternatives
      val arrMkStringSym = arrOpsTyp.member(TermName("mkString")).alternatives
      val arrMinSym = arrOpsTyp.member(TermName("min")).alternatives
      val arrMaxSym = arrOpsTyp.member(TermName("max")).alternatives
      val arrReverseSym = arrOpsTyp.member(TermName("reverse")).alternatives
      val arrContainsSym = arrOpsTyp.member(TermName("contains")).alternatives
      val arrSliceSym = arrOpsTyp.member(TermName("slice")).alternatives
      val arrPlusPlusSym = arrOpsTyp.member(TermName("$plus$plus")).alternatives
      val arrFlattenSym = arrOpsTyp.member(TermName("flatten")).alternatives
      val arrIntersectSym = arrOpsTyp.member(TermName("intersect")).alternatives
      val arrDistinctSym = arrOpsTyp.member(TermName("distinct")).alternatives
      val arrFillSym = arrObjTyp.member(TermName("fill")).alternatives
      val mapPlusPlusSym = mapTyp.member(TermName("$plus$plus")).alternatives

      def unapply(t: mTree): Option[sparkexpr.Expression] =
        t match {
          case InstanceMethodCall(elem, args1, args2) =>
            if (arrSizeSym.contains(t.symbol)) {
              for (arrE <- CatalystExpression.unapply(elem)) yield sparkexpr.Size(arrE)
            } else if (arrZipSym.contains(t.symbol)) {
              for (lArr <- CatalystExpression.unapply(elem);
                   rArr <- CatalystExpression.unapply(args1(0))
                   ) yield sparkexpr.ArraysZip(Seq(lArr, rArr))
            } else if (arrMkStringSym.contains(t.symbol) && args1.size == 0) {
              for (arrE <- CatalystExpression.unapply(elem)) yield
                sparkexpr.ArrayJoin(arrE, sparkexpr.Literal(" "), None)
            } else if (arrMkStringSym.contains(t.symbol) && args1.size == 1) {
              for (arrE <- CatalystExpression.unapply(elem);
                   sepE <- CatalystExpression.unapply(args1(0))) yield
                sparkexpr.ArrayJoin(arrE, sepE, None)
            } else if (arrMinSym.contains(t.symbol)) {
              for (arrE <- CatalystExpression.unapply(elem)) yield sparkexpr.ArrayMin(arrE)
            } else if (arrMaxSym.contains(t.symbol)) {
              for (arrE <- CatalystExpression.unapply(elem)) yield sparkexpr.ArrayMax(arrE)
            } else if (arrReverseSym.contains(t.symbol)) {
              for (arrE <- CatalystExpression.unapply(elem)) yield sparkexpr.Reverse(arrE)
            } else if (arrContainsSym.contains(t.symbol)) {
              for (arrE <- CatalystExpression.unapply(elem);
                   elemE <- CatalystExpression.unapply(args1(0))) yield
                sparkexpr.ArrayContains(arrE, elemE)
            } else if (arrSliceSym.contains(t.symbol)) {
              for (arrE <- CatalystExpression.unapply(elem);
                   fromE <- CatalystExpression.unapply(args1(0));
                   untilE <- CatalystExpression.unapply(args1(0))
                   ) yield
                sparkexpr.Slice(
                  arrE,
                  fromE,
                  sparkexpr.Add(sparkexpr.Subtract(untilE, fromE), sparkexpr.Literal(1))
                )
            } else if (arrPlusPlusSym.contains(t.symbol)) {
              for (lArr <- CatalystExpression.unapply(elem);
                   rArr <- CatalystExpression.unapply(args1(0))
                   ) yield sparkexpr.Concat(Seq(lArr, rArr))
            } else if (arrFlattenSym.contains(t.symbol)) {
              for (arrE <- CatalystExpression.unapply(elem)) yield sparkexpr.Flatten(arrE)
            } else if (arrIntersectSym.contains(t.symbol)) {
              for (lArr <- CatalystExpression.unapply(elem);
                   rArr <- CatalystExpression.unapply(args1(0))
                   ) yield sparkexpr.ArrayIntersect(lArr, rArr)
            } else if (arrDistinctSym.contains(t.symbol)) {
              for (arrE <- CatalystExpression.unapply(elem)) yield sparkexpr.ArrayDistinct(arrE)
            } else if (mapPlusPlusSym.contains(t.symbol) && args1.size == 1) {
              for (mapE <- CatalystExpression.unapply(elem);
                   oMapE <- CatalystExpression.unapply(args1(0))) yield
                sparkexpr.MapConcat(Seq(mapE, oMapE))
            } else None
          case ModuleMethodCall(id, args, args2) =>
            if (arrFillSym.contains(id)) {
              for (countE <- CatalystExpression.unapply(args(0));
                   elemE <- CatalystExpression.unapply(args2(0))
                   ) yield sparkexpr.ArrayRepeat(elemE, countE)
            } else None
          case _ => None
        }
    }

    object CollectionUtilsFunctions {
      val collUtilsTyp = typeOf[CollectionUtils.type]

      // TODO: MapKeys, MapValues <- CollectUtils; MapConcat
      val mapEntriesSym = collUtilsTyp.member(TermName("mapEntries")).alternatives
      val mapFromEntriesSym = collUtilsTyp.member(TermName("mapFromEntries")).alternatives

      val sortArraySym = collUtilsTyp.member(TermName("sortArray")).alternatives
      val shuffleSym = collUtilsTyp.member(TermName("shuffleArray")).alternatives
      val overlapSym = collUtilsTyp.member(TermName("overlapArrays")).alternatives
      val positionSym = collUtilsTyp.member(TermName("positionArray")).alternatives
      val sequenceSym = collUtilsTyp.member(TermName("sequence")).alternatives
      val date_sequenceSym = collUtilsTyp.member(TermName("date_sequence")).alternatives
      val timestamp_sequenceSym = collUtilsTyp.member(TermName("timestamp_sequence")).alternatives
      val removeSym = collUtilsTyp.member(TermName("removeArray")).alternatives
      val exceptSym = collUtilsTyp.member(TermName("exceptArray")).alternatives
      val mapKeysSym = collUtilsTyp.member(TermName("mapKeys")).alternatives
      val mapValuesSym = collUtilsTyp.member(TermName("mapValues")).alternatives

      def unapply(t: mTree): Option[sparkexpr.Expression] =
        t match {
          case ModuleMethodCall(id, args, args2) =>
            if (mapEntriesSym.contains(id.symbol)) {
              for (
                mE <- CatalystExpression.unapply(args(0))
              ) yield sparkexpr.MapEntries(mE)
            } else if (mapFromEntriesSym.contains(id.symbol)) {
              for (
                aE <- CatalystExpression.unapply(args(0))
              ) yield sparkexpr.MapFromEntries(aE)
            } else if (sortArraySym.contains(id.symbol)) {
              for (
                aE <- CatalystExpression.unapply(args(0));
                sE <- CatalystExpression.unapply(args(1))
              ) yield sparkexpr.SortArray(aE, sE)
            } else if (shuffleSym.contains(id.symbol)) {
              (for (
                argEs <- CatalystExpressions.unapplySeq(args)
              ) yield if (args.size == 1) {
                Some(sparkexpr.Shuffle(argEs.head))
              } else if (args.last.isInstanceOf[sparkexpr.Literal]) {
                Some(sparkexpr.Shuffle(argEs.head,
                  Some(argEs.last.asInstanceOf[sparkexpr.Literal].value.asInstanceOf[Long])
                ))
              } else None).flatten
            } else if (overlapSym.contains(id.symbol)) {
              for (
                lArr <- CatalystExpression.unapply(args(0));
                rArr <- CatalystExpression.unapply(args(1))
              ) yield sparkexpr.ArraysOverlap(lArr, rArr)
            } else if (positionSym.contains(id.symbol)) {
              for (
                arrE <- CatalystExpression.unapply(args(0));
                elemE <- CatalystExpression.unapply(args(1))
              ) yield sparkexpr.ArrayPosition(arrE, elemE)
            } else if (sequenceSym.contains(id.symbol)) {
              for (
                startE <- CatalystExpression.unapply(args(0));
                stopE <- CatalystExpression.unapply(args(1));
                stepE <- CatalystExpression.unapply(args(2))
              ) yield sparkexpr.Sequence(startE, stopE, Some(stepE), None)
            } else if (date_sequenceSym.contains(id.symbol)) {
              for (
                startE <- CatalystExpression.unapply(args(0));
                stopE <- CatalystExpression.unapply(args(1));
                stepE <- CatalystExpression.unapply(args(2))
              ) yield sparkexpr.Sequence(startE, stopE, Some(stepE), None)
            } else if (timestamp_sequenceSym.contains(id.symbol)) {
              for (
                startE <- CatalystExpression.unapply(args(0));
                stopE <- CatalystExpression.unapply(args(1));
                stepE <- CatalystExpression.unapply(args(2))
              ) yield sparkexpr.Sequence(startE, stopE, Some(stepE), None)
            } else if (removeSym.contains(id.symbol)) {
              for (
                arrE <- CatalystExpression.unapply(args(0));
                elemE <- CatalystExpression.unapply(args(1))
              ) yield sparkexpr.ArrayRemove(arrE, elemE)
            } else if (exceptSym.contains(id.symbol)) {
              for (
                lArr <- CatalystExpression.unapply(args(0));
                rArr <- CatalystExpression.unapply(args(1))
              ) yield sparkexpr.ArrayExcept(lArr, rArr)
            } else if (mapKeysSym.contains(id.symbol)) {
              for (
                mapE <- CatalystExpression.unapply(args(0))
              ) yield sparkexpr.MapKeys(mapE)
            } else if (mapValuesSym.contains(id.symbol)) {
              for (
                mapE <- CatalystExpression.unapply(args(0))
              ) yield sparkexpr.MapValues(mapE)
            } else None
          case _ => None
        }
    }

    object CollName {
      def unapply(t: mTree):
      Option[mTermName] = t match {
        case Select(Ident(collNm), TermName("apply")) if collNm.isTermName =>
          Some(collNm.toTermName)
        case _ => None
      }
    }

    object GetEntryExpr {
      def unapply(vInfo: ValInfo, idxExpr: sparkexpr.Expression):
      Option[sparkexpr.Expression] = vInfo.typInfo.catalystType match {
        case a: ArrayType => Some(sparkexpr.GetArrayItem(vInfo.rhsExpr, idxExpr))
        case m: MapType => Some(sparkexpr.GetMapValue(vInfo.rhsExpr, idxExpr))
        case _ => None
      }
    }

    object CollectionApply {
      def unapply(t: mTree): Option[sparkexpr.Expression] =
        t match {
          case q"$id(..$args)" if args.size == 1 =>
            for (
              collNm <- CollName.unapply(id);
              vInfo <- scope.get(collNm);
              idxExpr <- CatalystExpression.unapply(args(0).asInstanceOf[mTree]);
              valExpr <- GetEntryExpr.unapply(vInfo, idxExpr)
            ) yield valExpr
          case _ => None
        }
    }

    object CollectionConstruct {
      val arrApplySym = arrObjTyp.decl(TermName("apply"))
      val mapApplySym = mapObjTyp.member(TermName("apply"))

      def unapply(t: mTree): Option[sparkexpr.Expression] =
        t match {
          case q"$id(..$args)" if arrApplySym.alternatives.contains(id.symbol) =>
            for (
              entries <- CatalystExpressions.unapplySeq(args)
            ) yield sparkexpr.CreateArray(entries)
          case q"$id(..$args)(..$implArgs)" if arrApplySym.alternatives.contains(id.symbol) =>
            for (
              entries <- CatalystExpressions.unapplySeq(args)
            ) yield sparkexpr.CreateArray(entries)
          case q"$id(..$args)" if mapApplySym.alternatives.contains(id.symbol) =>
            for (
              entries <- CatalystExpressions.unapplySeq(args)
              if entries.forall(_.isInstanceOf[sparkexpr.CreateNamedStruct])
            ) yield {
              val mEntries = entries.flatMap(_.asInstanceOf[sparkexpr.CreateNamedStruct].valExprs)
              sparkexpr.CreateMap(mEntries)
            }
          case _ => None
        }
    }

  }

}
