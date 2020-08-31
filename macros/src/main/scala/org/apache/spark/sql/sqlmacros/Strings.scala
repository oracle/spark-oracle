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
import org.apache.spark.sql.oracle.OraSparkUtils

trait Strings { self : ExprTranslator =>

  import macroUniverse._

  val strTyp = typeOf[String]
  val strOpsTyp = typeOf[scala.collection.immutable.StringOps]
  val strUtilsTyp = typeOf[StringUtils.type]

  val toUpperSym = strTyp.member(TermName("toUpperCase")).alternatives
  val toLowerSym = strTyp.member(TermName("toLowerCase")).alternatives
  val replaceSym = strTyp.member(TermName("replace")).alternatives
  val trimSym = strTyp.member(TermName("trim")).alternatives
  val indexOfSym = strTyp.member(TermName("indexOf")).alternatives
  val substringSym = strTyp.member(TermName("substring")).alternatives
  val lengthSym = strTyp.member(TermName("length")).alternatives
  val startsWithSyms = strTyp.member(TermName("startsWith")).alternatives
  val endsWithSym = strTyp.member(TermName("endsWith"))
  val containsSym = strTyp.member(TermName("contains"))

  val strOpsTimesSym = strOpsTyp.member(TermName("$times")).alternatives

  val toConcatWSStrSym = strUtilsTyp.member(TermName("toConcatWSStr")).alternatives
  val toConcatWSArrStrSym = strUtilsTyp.member(TermName("toConcatWSArrStr")).alternatives
  val concatWsSym = strUtilsTyp.member(TermName("concatWs")).alternatives
  val eltSym = strUtilsTyp.member(TermName("elt")).alternatives
  val overlaySym = strUtilsTyp.member(TermName("overlay")).alternatives
  val translateSym = strUtilsTyp.member(TermName("translate")).alternatives

  /*
   * Notes:
   * 1. String.replace/replaceAll cannot be translated because
   *    StringReplace replaces all occurrences of `search` with `replace`."
   */

  /*
    functions TODO:

    ConcatWS(sep, Array[String] | String,...)   <- Array[String].mkString(sep)
    Elt(idx, child1, child2, ...)

    Upper(child) <- str.toUpper
    Lower <- str.toLowerCase
    Contains <- str.contains
    StartsWith <- str.startsWith
    EndsWith <- str.endsWith

    StringReplace <- str.replace
    Overlay <- by a StringUtils func
    StringTranslate <- by a StringUtils func
    FindInSet <- by a StringUtils func

    StringTrim <- str.trim
    StringTrimLeft <- by a StringUtils func
    StringTrimRight <- by a StringUtils func

    StringInstr <- str.indexOf

    SubstringIndex <- by a StringUtils func
    StringLocate <- by a StringUtils func

    StringLPad <- by a StringUtils func
    StringRPad <- by a StringUtils func

    ParseUrl <- by a URLUtils

    FormatString <- no translate

    InitCap <- by a StringUtils func

    StringRepeat <- str * int
    StringSpace <- by a StringUtils func

    Substring <- str.substring
    Right <- by a StringUtils func
    Left <- by a StringUtils func

    Length <- str.length
    BitLength <- by a StringUtils func
    OctetLength <- by a StringUtils func
    Levenshtein <- by a StringUtils func
    SoundEx <- by a StringUtils func
    Ascii <- by a StringUtils func

    Chr <- by a StringUtils func
    Base64 <- by a StringUtils func
    UnBase64 <- by a StringUtils func
    Decode <- by a StringUtils func
    Encode <- by a StringUtils func
    FormatNumber <- by a StringUtils func
    Sentences <- by a StringUtils func

    Concat(in collectionOps.scala) <- "a" + "b"
   */

  object StringPatterns {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      t match {
        case InstanceMethodCall(elem, args1, args2) =>
          if (toUpperSym.contains(t.symbol)) {
            for (strE <- CatalystExpression.unapply(elem)) yield sparkexpr.Upper(strE)
          } else if (toLowerSym.contains(t.symbol) ) {
            for (strE <- CatalystExpression.unapply(elem)) yield sparkexpr.Lower(strE)
          } else if (trimSym.contains(t.symbol)) {
            for (strE <- CatalystExpression.unapply(elem)) yield sparkexpr.StringTrim(strE)
          } else if (indexOfSym.contains(t.symbol)) {
            for (
              strE <- CatalystExpression.unapply(elem);
              argE <- CatalystExpression.unapply(args1(0)) if args1(0).tpe <:< strTyp
            ) yield sparkexpr.StringInstr(strE, argE)
          } else if (substringSym.contains(t.symbol) && args1.size == 2) {
            for (
              strE <- CatalystExpression.unapply(elem);
              posE <- CatalystExpression.unapply(args1(0)) if args1(0).tpe <:< typeOf[Int];
              lenE <- CatalystExpression.unapply(args1(1)) if args1(1).tpe <:< typeOf[Int]
            ) yield sparkexpr.Substring(strE, posE, lenE)
          } else if (substringSym.contains(t.symbol)) {
            for (
              strE <- CatalystExpression.unapply(elem);
              posE <- CatalystExpression.unapply(args1(0)) if args1(0).tpe <:< typeOf[Int]
            ) yield sparkexpr.Substring(strE, posE, sparkexpr.Literal(Int.MaxValue))
          } else if (lengthSym.contains(t.symbol)) {
            for (strE <- CatalystExpression.unapply(elem)) yield sparkexpr.Length(strE)
          } else None
        case ModuleMethodCall(id, args1, args2) =>
          if (eltSym.contains(id.symbol)  && args1.size > 1) {
            for (
              nE <- CatalystExpression.unapply(args1(0));
              inputsE <- CatalystExpressions.unapplySeq(args1.tail)
            ) yield sparkexpr.Elt(nE +: inputsE)
          } else if (concatWsSym.contains(id.symbol) && args1.size > 1) {
            for (
              sepE <- CatalystExpression.unapply(args1(0));
              inputsE <- ConcatWSArgs.unapplySeq(args1.tail)
            ) yield sparkexpr.ConcatWs(sepE +: inputsE)
          } else None
        case q"$l(..$str).$m(..$args)" if strOpsTimesSym.contains(t.symbol) =>
            for (
              timesE <- CatalystExpression.unapply(args(0)) if args(0).tpe <:< typeOf[Int];
              strE <- CatalystExpression.unapply(str(0)) if str(0).tpe <:< typeOf[String]
            ) yield sparkexpr.StringRepeat(strE, timesE)
        case _ => None
      }
  }

  object ConcatWSArgs {

    def unapplySeq(tS: Seq[mTree]): Option[Seq[sparkexpr.Expression]] =
      OraSparkUtils.sequence(tS.map(ConcatWSArg.unapply(_)))

    object ConcatWSArg {
      def unapply(t: mTree): Option[sparkexpr.Expression] = t match {
        case q"$id(..$args)" if toConcatWSStrSym.contains(id.symbol) && args.size == 1 =>
          CatalystExpression.unapply(args(0))
        case q"$id(..$args)" if toConcatWSArrStrSym.contains(id.symbol) && args.size == 1 =>
          CatalystExpression.unapply(args(0))
        case _ => None
      }
    }
  }
}
