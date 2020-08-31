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

package org.apache.spark.sql.oracle

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.types.StringType

/**
 * Represents a sql string with bind values.
 * Provides a composition API to compose component snippets into larger snippets.
 * Based on `SQLSyntax` in [[http://scalikejdbc.org/]]
 *
 * @param sql
 * @param params
 */
class SQLSnippet private (val sql: String, val params: Seq[Literal]) {

  import OraSQLImplicits._
  import SQLSnippet._

  override def equals(that: Any): Boolean = {
    that match {
      case sqlSnip: SQLSnippet =>
        sql == sqlSnip.sql && params == sqlSnip.params
      case _ =>
        false
    }
  }

  override def hashCode: Int = (sql, params).##

  override def toString(): String = s"SQLSnippet(value: ${sql}, parameters: ${params})"

  def append(syntax: SQLSnippet): SQLSnippet = osql"${this} ${syntax}"
  def +(syntax: SQLSnippet): SQLSnippet = this.append(syntax)
  def ++(snips: Seq[SQLSnippet]): SQLSnippet = snips.foldLeft(this)(_ + _)

  def groupBy(columns: SQLSnippet*): SQLSnippet =
    if (columns.isEmpty) this else osql"${this}${nl}group by ${csv(columns: _*)}"

  def groupBy(gByListOpt: Option[Seq[SQLSnippet]]): SQLSnippet = {
    gByListOpt.fold(this) { columns => groupBy(columns : _*) }
  }

  def having(condition: SQLSnippet): SQLSnippet = osql"${this}${nl}having ${condition}"

  def orderBy(oByListOpt: Option[Seq[SQLSnippet]]): SQLSnippet =
    oByListOpt.fold(this) { columns => orderBy(columns : _*) }

  def orderBy(columns: SQLSnippet*): SQLSnippet =
    if (columns.isEmpty) this else osql"${this}${nl}order by ${csv(columns: _*)}"

  def asc: SQLSnippet = osql"${this} asc"
  def desc: SQLSnippet = osql"${this} desc"
  def limit(n: Int): SQLSnippet = osql"${this} limit ${n}"
  def offset(n: Int): SQLSnippet = osql"${this} offset ${n}"

  def from(oraTbl: OraTable): SQLSnippet = from(SQLSnippet.tableQualId(oraTbl))
  def from(sql : SQLSnippet) : SQLSnippet = osql"${this}${nl}from ${sql}"

  def where: SQLSnippet = osql"${this} where"
  def where(where: SQLSnippet): SQLSnippet = osql"${this}${nl}where ${where}"
  def where(whereOpt: Option[SQLSnippet]): SQLSnippet = whereOpt.fold(this)(where(_))

  def and: SQLSnippet = osql"${this} and"
  def and(sqlSnip: SQLSnippet): SQLSnippet = osql"$this and ($sqlSnip)"
  def and(andOpt: Option[SQLSnippet]): SQLSnippet = andOpt.fold(this)(and(_))
  def or: SQLSnippet = osql"${this} or"
  def or(sqlSnip: SQLSnippet): SQLSnippet = osql"$this or ($sqlSnip)"
  def or(orOpt: Option[SQLSnippet]): SQLSnippet = orOpt.fold(this)(or(_))

  /*
   * Decided not to refactor eq, neq, gt, ... into calling a common helper
   * method predicate because generating sql in this common method
   * would require calling [[SQLSnippet.join]]; which would incur
   * cost of building q Seq.
   *
   */

  private def sqlSnippet[T](v: T)(implicit lb: OraSQLLiteralBuilder[T]): SQLSnippet = v match {
    case sqlSnip: SQLSnippet => sqlSnip
    case l: Literal => osql"${lb(v)}"
  }

  def eq[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} = ${sqlSnippet(v)}"

  def neq[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} != ${sqlSnippet(v)}"

  def `<`[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} < ${sqlSnippet(v)}"

  def `<=`[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} <= ${sqlSnippet(v)}"

  def `>`[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} > ${sqlSnippet(v)}"

  def `>=`[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} >= ${sqlSnippet(v)}"

  def isNull: SQLSnippet = osql"${this} is null"
  def isNotNull: SQLSnippet = osql"${this} is not null"

  def between[A, B](a: A, b: B)(
      implicit lbA: OraSQLLiteralBuilder[A],
      lbB: OraSQLLiteralBuilder[B]): SQLSnippet =
    osql"${this} between ${sqlSnippet(a)} and ${sqlSnippet(b)}"

  def not: SQLSnippet = osql"not ${this}"

  def in[A](as: A*)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet = {
    val inlist = join(as.map(sqlSnippet(_)), comma, false)
    osql"${this} in (${inlist})"
  }

  def notIn[A](as: A*)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet = {
    val inlist = join(as.map(sqlSnippet(_)), comma, false)
    osql"${this} not in (${inlist})"
  }

  def as(alias: String): SQLSnippet = this + AS + colRef(alias)
}

/**
 * an entity that has a [[SQLSnippet]] representation.
 */
trait SQLSnippetProvider {

  def orasql: SQLSnippet
}

trait OraSQLImplicits {

  import SQLSnippet.SQLSnippetInterpolationString

  /**
   * Enables sql interpolation.
   *
   * {{{
   *   osql"select * from sales"
   *   val whereClause = osql"where quantity > ${qVal}"
   *   osql"select * from sales ${whereClause}"
   * }}}
   *
   * But important:
   * - the params of interpolation are treated as bind values, so cannot use params
   *   to inject sql. Use the compositional methods of [[SQLSnippet]] to build up sql.
   *   - For example:
   *     this `val verb = "select"; osql"${verb} 1 from dual` will generate
   *     the sql: `? 1 from dual` with a bind param of `Seq(Literal("select"))`
   *     which is not valid sql.
   */
  @inline implicit def oraSQLInterpolation(s: StringContext): SQLSnippetInterpolationString =
    new SQLSnippetInterpolationString(s)

}

object OraSQLImplicits extends OraSQLImplicits

object SQLSnippet {

  import OraSQLImplicits._

  val empty: SQLSnippet = osql""
  val nl: SQLSnippet = apply(System.lineSeparator(), Seq.empty)
  val comma: SQLSnippet = osql","
  val dot: SQLSnippet = osql"."
  val CASE: SQLSnippet = osql"CASE"
  val WHEN: SQLSnippet = osql"WHEN"
  val THEN: SQLSnippet = osql"THEN"
  val ELSE: SQLSnippet = osql"ELSE"
  val END: SQLSnippet = osql"END"
  val AS: SQLSnippet = osql"AS"
  val IN: SQLSnippet = osql"IN"
  val NOT_IN: SQLSnippet = osql"NOT IN"
  val LPAREN: SQLSnippet = osql"("
  val RPAREN: SQLSnippet = osql")"
  val AND: SQLSnippet = osql"and"
  val LTE: SQLSnippet = osql"<="
  val EXISTS: SQLSnippet = osql"exists"
  val NOT_EXISTS: SQLSnippet = osql"not exists"
  val PARTITION_BY = osql"PARTITION BY"
  val ORDER_BY = osql"ORDER BY"

  def literalSnippet(s: String): SQLSnippet =
    SQLSnippet(s, Seq.empty)

  def literalSnippet(s: Literal): SQLSnippet = {
    assert(s.dataType == StringType)
    SQLSnippet(s.value.toString, Seq.empty)
  }

  def unaryOp(op: String, child: SQLSnippet): SQLSnippet = {
    val opSnip = literalSnippet(op)
    osql"${opSnip}${child}"
  }

  def postfixUnaryOp(op: String, child: SQLSnippet): SQLSnippet = {
    val opSnip = literalSnippet(op)
    osql"${child} ${opSnip}"
  }

  def call(fn: String, args: SQLSnippet*): SQLSnippet = {
    val fnSnip = literalSnippet(fn)
    osql"$fnSnip(${join(args, comma, true)})"
  }
  def operator(op: String, args: SQLSnippet*): SQLSnippet = {
    val opSnip = literalSnippet(op)
    osql"(${join(args, opSnip, true)})"
  }

  def simpleCase(
      cases: Seq[(SQLSnippet, SQLSnippet)],
      elseCase: Option[SQLSnippet]): SQLSnippet = {
    val caseSnips = for ((caseCond, caseValue) <- cases) yield {
      WHEN + caseCond + THEN + caseValue
    }
    val elseSnip = elseCase.map(ELSE + _).getOrElse(empty)
    CASE ++ caseSnips + elseSnip + END
  }

  def searchedCase(
      cases: Seq[(SQLSnippet, SQLSnippet)],
      elseCase: Option[SQLSnippet]): SQLSnippet = {
    val caseSnips = for ((caseCond, caseValue) <- cases) yield {
      WHEN + caseCond + THEN + caseValue
    }
    val elseSnip = elseCase.map(ELSE + _).getOrElse(empty)
    CASE ++ caseSnips + elseSnip + END
  }

  def unapply(snippet: SQLSnippet): Option[(String, Seq[Literal])] =
    Some((snippet.sql, snippet.params))

  private def apply(sql: String, params: Seq[Literal]) =
    new SQLSnippet(sql, params)

  def join(
      parts: collection.Seq[SQLSnippet],
      delimiter: SQLSnippet,
      spaceBeforeDelimiter: Boolean = true): SQLSnippet = {

    val sep = if (spaceBeforeDelimiter) {
      s" ${delimiter.sql} "
    } else {
      s"${delimiter.sql} "
    }

    val value = parts.collect { case p if p.sql.nonEmpty => p.sql }.mkString(sep)

    val parameters = if (delimiter.params.isEmpty) {
      parts.flatMap(_.params)
    } else {
      parts.tail.foldLeft(parts.headOption.fold(collection.Seq.empty[Literal])(_.params)) {
        case (params, part) => params ++ delimiter.params ++ part.params
      }
    }
    apply(value, parameters)
  }

  def csv(parts: SQLSnippet*): SQLSnippet = join(parts, comma, false)

  def select(projections: SQLSnippet*): SQLSnippet = osql"select ${csv(projections: _*)}"

  def combine(op: SQLSnippet, snippets: Option[SQLSnippet]*): Option[SQLSnippet] = {
    var isEmpty: Boolean = true
    val res = snippets.foldLeft(empty) {
      case (r, None) => r
      case (r, Some(s)) =>
        if (isEmpty) {
          isEmpty = false
          s
        } else {
          r + op + s
        }
    }
    if (isEmpty) None else Some(res)
  }

  /*
   * Shouldn't need this.
   * If we do there are problems:
   * - append, join are loose about adding spaces between snippets; so careful here
   * - optimize for case where parts.size = 1
   */
  private def qualifiedId(parts: Seq[String]): SQLSnippet = {
    val nms = parts.map(apply(_, Seq.empty))
    (nms.init.map(nm => osql"${nm}${dot}") :+ nms.last).foldLeft(empty)((r, o) => osql"${r}${o}")
  }

  def quotedQualifiedName(parts: Seq[String]): SQLSnippet = {
    val refs = parts.map(objRef(_))
    refs.init.foldRight(refs.last)((o, r) => osql"${o}.${r}")
  }

  def colRef(nm: String): SQLSnippet = {
    apply(s""""${nm}"""", Seq.empty)
  }

  def objRef(nm: String): SQLSnippet = {
    apply(s""""${nm}"""", Seq.empty)
  }

  def qualifiedColRef(qual : String, nm: String): SQLSnippet = {
    apply(s""""${qual}"."${nm}"""", Seq.empty)
  }

  def tableQualId(oraTbl: OraTable) : SQLSnippet =
    literalSnippet(s""""${oraTbl.schema}"."${oraTbl.name}"""")

  def subQuery(sql : SQLSnippet) : SQLSnippet = osql"( ${sql} )"

  /**
   * A [[SQLSnippet]] generator build from a scala String Interpolation
   * expression of the form `osql"..."`
   * @param s
   */
  class SQLSnippetInterpolationString(private val s: StringContext) extends AnyVal {

    /**
     * - The `params` can be [[Literal]], a [[SQLSnippet]], a [[SQLSnippetProvider]]
     *   or 'any value'
     *   - any values are converted to [[Literal]]; but we only support certain
     *     dataTypes. See [[OraSQLLiteralBuilder.isSupportedForBind]]
     *   - [[SQLSnippetProvider]] is replaced by its [[SQLSnippet]]
     * - generates a [[SQLSnippet]]
     *   - by gathering all parts from the [[StringContext] and snippets
     *     from any [[SQLSnippet]] params into a new snippet.
     *   - gathering all lietral params and flattening params from [[SQLSnippet]]
     *     params into a new param list.
     * @param params
     * @return
     */
    def osql(params: Any*): SQLSnippet = {
      val normalizedParams = normalizeValues(params.map(util.makeImmutable)).toSeq
      SQLSnippet(buildQuery(normalizedParams), buildParams(normalizedParams))
    }

    private def buildQuery(params: Iterable[Any]): String = {
      val sb = new StringBuilder

      def addPlaceholder(param: Any): StringBuilder = param match {
        case l: Literal => sb += '?'
        case SQLSnippet(sql, _) => sb ++= sql
        case LastParam => sb
      }

      for ((qp, param) <- s.parts.zipAll(params, "", LastParam)) {
        sb ++= qp
        addPlaceholder(param)
      }
      sb.result()
    }

    /**
     * - Replace [[SQLSnippetProvider]] with its [[SQLSnippet]]
     * - Replace `any value` with a [[Literal]] representation.
     */
    private def normalizeValues(params: Traversable[Any]): Traversable[Any] = {
      for (p <- params) yield {
        p match {
          case l: Literal => OraSQLLiteralBuilder.literalOf(l)
          case ss: SQLSnippet => ss
          case ssp: SQLSnippetProvider => ssp.orasql
          case p => OraSQLLiteralBuilder.literalOf(p)
        }
      }
    }

    private def buildParams(params: Traversable[Any]): Seq[Literal] = {
      val lBuf = ArrayBuffer[Literal]()

      def add(p: Any): Unit = p match {
        case l: Literal => lBuf += l
        case ss: SQLSnippet => lBuf ++= ss.params
      }

      for (p <- params) {
        add(p)
      }
      lBuf
    }
  }

  private case object LastParam

}
