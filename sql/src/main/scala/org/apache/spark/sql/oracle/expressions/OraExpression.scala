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

package org.apache.spark.sql.oracle.expressions

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{And, Coalesce, Expression, IsNotNull, IsNull, Literal}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.oracle.{OraSparkUtils, OraSQLImplicits, SQLSnippet, SQLSnippetProvider}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.unsafe.types.UTF8String

abstract class OraExpression extends TreeNode[OraExpression]
  with OraSQLImplicits with SQLSnippetProvider {

  def catalystExpr: Expression

  def orasql: SQLSnippet

  def reifyLiterals : OraExpression = this transformUp {
    case l : OraLiteral => l.toLiteralSql
  }

  /*
   * When showing `OraExpression` such as in the treeString of
   * `OraPlan` just show the wrapped `catalystExpr`
   */
  override def stringArgs: Iterator[Any] = Iterator(catalystExpr)

  /*
   * Copy behavior for String and treeString from [[Expression]]
   * `treeString` of a OraPlan will show `OraExpression` similar
   * to how spark `Expression` are shown in treeString of
   * spark `QueryPlan`.
   */

  def prettyName: String = nodeName.toLowerCase(Locale.ROOT)

  protected def flatArguments: Iterator[Any] = stringArgs.flatMap {
    case t: Iterable[_] => t
    case single => single :: Nil
  }

  // Marks this as final, Expression.verboseString should never be called, and thus shouldn't be
  // overridden by concrete classes.
  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleString(maxFields: Int): String = toString

  override def toString: String = prettyName + truncatedString(
    flatArguments.toSeq, "(", ", ", ")", SQLConf.get.maxToStringFields)

  override def simpleStringWithNodeId(): String = {
    throw new UnsupportedOperationException(
      s"$nodeName does not implement simpleStringWithNodeId")
  }

}

trait OraLeafExpression { self: OraExpression =>
  val children: Seq[OraExpression] = Seq.empty
}

case class OraUnaryOpExpression(op: String, catalystExpr: Expression, child: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(child)
  override def orasql: SQLSnippet = SQLSnippet.unaryOp(op, child.orasql)
}

case class OraPostfixUnaryOpExpression(op: String, catalystExpr: Expression, child: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(child)
  override def orasql: SQLSnippet = SQLSnippet.postfixUnaryOp(op, child.orasql)
}

case class OraNoArgFnExpression(fn: String, catalystExpr: Expression)
  extends OraExpression {
  val children: Seq[OraExpression] = Seq.empty
  override def orasql: SQLSnippet = SQLSnippet.call(fn)
}

case class OraUnaryFnExpression(fn: String, catalystExpr: Expression, child: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(child)
  override def orasql: SQLSnippet = SQLSnippet.call(fn, child.orasql)
}

case class OraBinaryOpExpression(
    op: String,
    catalystExpr: Expression,
    left: OraExpression,
    right: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(left, right)
  override def orasql: SQLSnippet = SQLSnippet.operator(op, left.orasql, right.orasql)
}

case class OraBinaryFnExpression(
    fn: String,
    catalystExpr: Expression,
    left: OraExpression,
    right: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(left, right)
  override def orasql: SQLSnippet = SQLSnippet.call(fn, left.orasql, right.orasql)
}

case class OraFnExpression(fn: String, catalystExpr: Expression, children: Seq[OraExpression])
    extends OraExpression {
  private def cSnips = children.map(_.orasql)
  override def orasql: SQLSnippet = SQLSnippet.call(fn, cSnips: _*)
}

object OraExpression {

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case OraLiterals(oE) => oE
      case Arithmetic(oE) => oE
      case Strings(oE) => oE
      case DateTime(oE) => oE
      case Conditional(oE) => oE
      case Named(oE) => oE
      case Predicates(oE) => oE
      case Nulls(oE) => oE
      case Subquery(oE) => oE
      case Casts(oE) => oE
      case Aggregates(oE) => oE
      case Sorts(oE) => oE
      case Windows(oE) => oE
      case OraNativeFunctions(oE) => oE
      case IgnoreExpressions(oE) => oE
      case _ => null
    })

  /*
   * Why pass in [[OraTable]]?
   * In general converting from catalyst Expression to OraExpression
   * we rely on catalyst Expression being resolved, which implies
   * we can assume an AttributeRef refers to a valid column in the
   * input OraPlan.
   *
   * This should be the case for Filter expressions also.
   * But providing the oracle scheme as a potential means to check.
   *
   * May remove this as wiring and constraints solidify.
   */
  def convert(fil: Filter, oraTab: OraTable): Option[OraExpression] = {
    // TODO
    None
  }

  def convert(expr: Expression): Option[OraExpression] = {
    unapply(expr)
  }

  def convert(exprs: Seq[Expression]): Option[OraExpression] = {
    val oEs = exprs.map(convert(_)).collect {
      case Some(oE) => oE
    }

    if (oEs.nonEmpty) {
      Some(oEs.tail.foldLeft(oEs.head) {
        case (left, right) =>
          OraBinaryOpExpression(AND, And(left.catalystExpr, right.catalystExpr), left, right)
      })
    } else {
      None
    }
  }

  /**
   * @see [[OraBooleanAsInt]]
   * @param oE
   * @return
   */
  def applyIntConversion(oE : OraExpression) : OraExpression = oE match {
    case oe if oe.catalystExpr.dataType == BooleanType => OraBooleanAsInt(oe)
    case oe => oe
  }

  /**
   * A `IsNotNull(IsNull(_))` expression gets translated to
   * something like `"C_INT" IS NULL IS NOT NULL`
   * which is invalid oracle sql and also unnecessary.
   * So we simply filter it out.
   *
   * @param oEs
   * @return
   */
  def removeInvalidNullCheck(oEs : Seq[OraExpression]) : Seq[OraExpression] = {
    oEs filterNot { oe =>
      oe.catalystExpr match {
        case IsNotNull(IsNull(_)) => true
        case _ => false
      }
    }
  }

  /**
   *  In oracle sql '''Coalesce with an empty string seems to be treated differently.'''
   *
   * '''Example 1:'''
   * - the following returns no rows:
   * {{{
   * select 1 from dual where coalesce(null, '') = coalesce(null, '’);
   * }}}
   *
   * '''Example 2:'''
   * The queries below return 2, except the last query.
   * Table:
   * {{{
   * create table hb_xyz as select 'a' ln from dual union select null from dual;
   * SQL> describe hb_xyz;
   *  Name        Null?    Type
   *  ----------------------------------------- -------- ----------------------------
   *  LN          CHAR(1)
   * }}}
   *
   *
   * '''Queries:'''
   * {{{
   * select count(*)
   * from (
   * select coalesce(ln , '') from hb_xyz
   * intersect
   * select coalesce(ln , '') from hb_xyz
   * );
   *
   * select count(*)
   * from (
   * select coalesce(ln , '<>') from hb_xyz
   * intersect
   * select coalesce(ln , '<>') from hb_xyz
   * );
   *
   * select count(*)
   * from (
   * select coalesce(ln , '<>')
   * from hb_xyz
   * where coalesce(ln , '<>')  in (select coalesce(ln , '<>') from hb_xyz)
   * );
   *
   * select count(*)
   * from (
   * select coalesce(ln , '')
   * from hb_xyz
   * where coalesce(ln , '')  in (select coalesce(ln , '') from hb_xyz)
   * )
   *
   * Plan for intersect queries:
   * Execution Plan
   * ----------------------------------------------------------
   * Plan hash value: 15181038
   *
   * --------------------------------------------------------------------------------
   * | Id  | Operation          | Name   | Rows  | Bytes | Cost (%CPU)| Time     |
   * --------------------------------------------------------------------------------
   * |   0 | SELECT STATEMENT      |        |     1 |       |     8    (25)| 00:00:01 |
   * |   1 |  SORT AGGREGATE       |        |     1 |       |        |           |
   * |   2 |   VIEW              |        |     2 |       |     8    (25)| 00:00:01 |
   * |   3 |    INTERSECTION       |        |       |       |        |           |
   * |   4 |     SORT UNIQUE       |        |     2 |     4 |     4    (25)| 00:00:01 |
   * |   5 |      TABLE ACCESS FULL| HB_XYZ |     2 |     4 |     3     (0)| 00:00:01 |
   * |   6 |     SORT UNIQUE       |        |     2 |     4 |     4    (25)| 00:00:01 |
   * |   7 |      TABLE ACCESS FULL| HB_XYZ |     2 |     4 |     3     (0)| 00:00:01 |
   * --------------------------------------------------------------------------------
   * Plan for in queries:
   * Execution Plan
   * ----------------------------------------------------------
   * Plan hash value: 3742104981
   *
   * ------------------------------------------------------------------------------
   * | Id  | Operation        | Name   | Rows  | Bytes | Cost (%CPU)| Time     |
   * ------------------------------------------------------------------------------
   * |   0 | SELECT STATEMENT    |         |       1 |       4 |       6   (0)| 00:00:01 |
   * |   1 |  SORT AGGREGATE     |         |       1 |       4 |          |         |
   * |*  2 |   HASH JOIN SEMI    |         |       1 |       4 |       6   (0)| 00:00:01 |
   * |   3 |    TABLE ACCESS FULL| HB_XYZ |       2 |       4 |       3   (0)| 00:00:01 |
   * |   4 |    TABLE ACCESS FULL| HB_XYZ |       2 |       4 |       3   (0)| 00:00:01 |
   * ------------------------------------------------------------------------------
   * Predicate Information (identified by operation id):
   * ---------------------------------------------------
   *
   *    2 - access(COALESCE("LN",'')=COALESCE("LN",'’))
   * OR
   *  2 - access(COALESCE("LN",'<>')=COALESCE("LN",'<>'))
   * }}}
   *
   * @param oE input OraExpression
   * @return
   */
  def fixEmptyStringCoalesce(oE : OraExpression) : OraExpression = (oE, oE.catalystExpr) match {
    case (oE@OraFnExpression(COALESCE, _, childrenOEs),
    cE@Coalesce(_ :: Literal(v, StringType) :: Nil)) if v.toString == "" =>
      val newChildrenOEs = (
        childrenOEs.init :+
          OraLiteral(Literal(UTF8String.fromString("<>"), StringType))
        ).toArray
      OraFnExpression(COALESCE, cE, newChildrenOEs)
    case (_, _) => oE
  }

  def fixForJoinCond(oE : OraExpression) : OraExpression =
    (fixEmptyStringCoalesce _ andThen applyIntConversion _)(oE)

  def transformEmptyStringCoalesce(oE : OraExpression) : OraExpression = oE transformUp {
    case oE : OraFnExpression => fixEmptyStringCoalesce(oE)
  }

  def canBeNull(cE : Expression) : Boolean = cE match {
    case n : IsNull => false
    case n : IsNotNull => false
    case c : Coalesce => false
    case _ => true
  }
}

object OraExpressions {
  def unapplySeq(eS: Seq[Expression]): Option[Seq[OraExpression]] =
    OraSparkUtils.sequence(eS.map(OraExpression.unapply(_)))

}
