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

package org.apache.spark.sql.connector.catalog.oracle

import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

import oracle.spark.ORASQLUtils.performDSQuery

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Unevaluable, UnevaluableAggregate, UserDefinedExpression}
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.expressions.{JDBCGetSet, OraLiterals}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

trait OraFunctionDefs { self : OracleMetadata.type =>

  case class OraFuncArg(name : String, dataType: OraDataType) {
    override def toString: String = s"${name}:${dataType.catalystType.toString}"
  }


  case class OraFunctionSignature(subProgramId : Int,
                                  retType : OraDataType,
                                  args : IndexedSeq[OraFuncArg]) {

    def this(subProgramId : Int,
             args : ArrayBuffer[OraFuncArg]) =
      this(subProgramId, args.head.dataType, args.tail.toIndexedSeq)

    override def toString: String =
      s"  subProgramId=${subProgramId}, retType=${retType.catalystType.toString}," +
        s" args={${args.mkString(",")}}"
  }

  private def fixOraFnNames(owner : String,
                            packageName : Option[String],
                            name : String) : Seq[String] = {
    (owner, packageName, name) match {
      case (ow, Some(p), n)
        if ow.toUpperCase(Locale.ROOT) == "SYS" &&
          p.toUpperCase(Locale.ROOT) == "STANDARD" &&
          n.toUpperCase(Locale.ROOT) == "USER" =>
        Seq("USER")
      case _ => Option(owner).toSeq ++ packageName.toSeq ++ Seq(name)
    }
  }

  case class OraFuncDef(packageName : Option[String],
                        owner : String,
                        name : String,
                        isAggregate : Boolean,
                        sigs : IndexedSeq[OraFunctionSignature]) {

    val qualNm = s"${packageName.map(_ + ".").getOrElse("")}${name}"

    lazy val orasql_fnname : String = {
      val qualifiedNm = fixOraFnNames(owner, packageName, name)
      SQLSnippet.quotedQualifiedName(qualifiedNm).sql
    }

    override def toString: String =
      s"""name=${qualNm},isAggregate=${isAggregate}
         |${sigs.mkString("\n")}""".stripMargin
  }

}

/**
 * Load ''Oracle Function'' metadata into a [[OraFunctionDefs.OraFuncDef]]
 * Oracle Function metadata is read from the `ALL_PROCEDURE` and `ALL_ARGUMENTS` table.
 * We attempt to load each ''subProgram'' entry into a [[OraFunctionDefs.OraFunctionSignature]].
 * A function must meet the following constraints:
 * - its return type and argument types must be one of the supported types, see [[OraDataType]].
 * - only `IN` arguments are allowed.
 * - for a given `subProgram` and argument position only 1 row(entry) must exist in the
 * `ALL_ARGUMENTS` table.
 *
 * For a function we return a [[OraFunctionDefs.OraFuncDef]] if at least 1 subProgram can be loaded
 * into a [[OraFunctionDefs.OraFunctionSignature]]. Details of subprograms that we fail to map are
 * logged at the INFO level.
 */
trait OraFunctionDefLoader { self : OracleMetadataManager =>

  import OracleMetadata._

  private[oracle] def loadFunctionDef(packageName : Option[String],
                                      funcName : String) : OraFuncDef = {

    val funcSigs = ArrayBuffer[OraFunctionSignature]()
    val issues = ArrayBuffer[String]()
    var isAgg : Option[String] = None
    var owner : String = null

    val strJdbcGetSet = OraLiterals.jdbcGetSet(StringType).asInstanceOf[JDBCGetSet[String]]
    val intJdbcGetSet = OraLiterals.jdbcGetSet(IntegerType).asInstanceOf[JDBCGetSet[Int]]


    performDSQuery(
      dsKey,
      """
        |select coalesce(p."OVERLOAD", 'None'), p."SUBPROGRAM_ID",
        |       "ARGUMENT_NAME", "POSITION",
        |       "DATA_TYPE", "DATA_LENGTH", "DATA_PRECISION", "DATA_SCALE",
        |       "IN_OUT", "AGGREGATE", p."OWNER",
        |       "TYPE_OWNER", "TYPE_NAME"
        |from "SYS"."ALL_PROCEDURES" p join "SYS"."ALL_ARGUMENTS" a on
        |      p."OBJECT_ID" = a."OBJECT_ID" and
        |      p."SUBPROGRAM_ID" = a."SUBPROGRAM_ID" and
        |      coalesce(p."OVERLOAD", 'null')  = coalesce(a."OVERLOAD", 'null')
        |where p."OBJECT_NAME" =  ? and
        |      coalesce(p."PROCEDURE_NAME", 'null') = coalesce(?, 'null') and
        |      p."OBJECT_TYPE" in ('FUNCTION', 'PROCEDURE', 'PACKAGE')
        |order by coalesce(p."OVERLOAD", 'None'), p."SUBPROGRAM_ID", "POSITION"
        |""".stripMargin,
      "retrieve function definition details",
      {ps =>
        val object_name = packageName.getOrElse(funcName)
        val procedure_name : String = if (packageName.isDefined) {
          funcName
        } else null
        ps.setString(1, object_name)
        ps.setString(2, procedure_name)
      }
    ) { rs =>
      var curr_subPgmId : Int = -1
      var prior_pos = -1
      var curr_errors : Boolean = false
      var currArgs = ArrayBuffer[OraFuncArg]()

      def addSig : Unit = {
        if (curr_subPgmId != -1 && !curr_errors) {
          funcSigs += new OraFunctionSignature(curr_subPgmId, currArgs)
        } else {
          issues += s"Function Overload(subProgram ${curr_subPgmId}) cannot be registered."
        }
      }

      def addIssue(issue : String) : Unit = {
        issues += issue
        curr_errors = true
      }

      while (rs.next()) {
        val subPgmId = intJdbcGetSet.readValue(rs, 2)
        val argNm = strJdbcGetSet.readValue(rs, 3)
        val inOut = strJdbcGetSet.readValue(rs, 9)
        val pos = intJdbcGetSet.readValue(rs, 4)

        if (!isAgg.isDefined) {
          isAgg = strJdbcGetSet.readOptionValue(rs, 10)
          owner = strJdbcGetSet.readValue(rs, 11)
        }

        breakable {

          if (subPgmId != curr_subPgmId) {
            if (curr_subPgmId != -1) {
              addSig
            }
            curr_subPgmId = subPgmId
            curr_errors = false
            prior_pos = -1
            currArgs = ArrayBuffer[OraFuncArg]()
          }

          if (pos == prior_pos) {
            addIssue(s"Not supported: subProgram ${curr_subPgmId} has multiple args for" +
              s" the same position: ${pos}")
            break
          } else if (pos > prior_pos + 1) {
            addIssue(s"Not supported: subProgram ${curr_subPgmId} missing arg at position" +
              s" ${prior_pos + 1}")
            prior_pos = pos
            break
          } else {
            prior_pos = pos
          }

          if (pos > 0 && inOut != "IN") {
            addIssue(s"Not supported: subProgram ${curr_subPgmId}, " +
              s"argument ${argNm}: is '${inOut}', only 'IN' arguments supported")
            break
          }

          val datatype = strJdbcGetSet.readValue(rs, 5)
          val length = intJdbcGetSet.readOptionValue(rs, 6)
          val precision = intJdbcGetSet.readOptionValue(rs, 7)
          val scale = intJdbcGetSet.readOptionValue(rs, 8)
          val udtOwner = strJdbcGetSet.readOptionValue(rs, 12)
          val udtTypNm = strJdbcGetSet.readOptionValue(rs, 13)
          val datatypeNm = OraDataType.dataTypeName(datatype, udtOwner, udtTypNm)
          val oDT = try {
             OraDataType.create(datatypeNm, length, precision, scale)
          } catch {
            case e : UnsupportedOraDataType =>
              addIssue(s"SubProgram ${curr_subPgmId}, Argument ${argNm} dataType is" +
                s" not supported: ${e.getMessage()}")
              break
          }
          currArgs += OraFuncArg(argNm, oDT)
        }
      }

      addSig

    }

    val qualNm = s"${packageName.map(_ + ".").getOrElse("")}${funcName}"

    if (funcSigs.nonEmpty) {

      if (issues.nonEmpty) {
        logInfo(s"Issues while loading function defintion '${qualNm}':\n${issues.mkString("\n")}")
      }

      val fnDef = OraFuncDef(packageName, owner, funcName, isAgg.getOrElse("NO") == "YES", funcSigs)

      logInfo(
        s"""Oracle function definition loaded for '${qualNm}':
           |$fnDef
           |""".stripMargin)

      fnDef

    } else {
      if (issues.isEmpty) {
        issues += "Check if (packageName, funcName) specified correctly"
      }

      invalidAction(s"Load function definition for $qualNm", Some(issues.mkString("\n")))
    }
  }
}

/**
 * A call to an oracle native function is resolved based on the argument
 * expressions at the call-site, by choosing the first signature whose argument datatypes
 * can be cast to the expressions at the call-site.
 *
 * @param fnDef
 */
class OraNativeRowFuncInvokeBuilder(val fnDef : OracleMetadata.OraFuncDef)
  extends Function1[Seq[Expression], Expression] {

  private def isMatch(args: Seq[Expression],
                      fnDef : OracleMetadata.OraFunctionSignature
                     ) : Boolean = {
    (args.size <= fnDef.args.size) &&
      args.zip(fnDef.args).forall(t => Cast.canCast(t._1.dataType, t._2.dataType.catalystType))
  }

  private def fnArgs(args: Seq[Expression],
                     sigIdx : Int) : Seq[Expression] = {
    val sig = fnDef.sigs(sigIdx)
    args.zip(sig.args).map {
      case (e, oA) => if (e.dataType == oA.dataType.catalystType) {
        e
      } else {
        Cast(e, oA.dataType.catalystType)
      }
    }
  }

  private def oraNativeFunc(fnDef : OracleMetadata.OraFuncDef,
                            sigIdx : Int,
                            children : Seq[Expression]) : Expression = {
    if (fnDef.isAggregate) {
      OraNativeAggFuncInvoke(fnDef, sigIdx, children)
    } else {
      OraNativeRowFuncInvoke(fnDef, sigIdx, children)
    }
  }

  override def apply(args: Seq[Expression]): Expression = {
    val sig = (0 until fnDef.sigs.size).find(i => isMatch(args, fnDef.sigs(i)))

    sig.
      map(i => oraNativeFunc(fnDef, i, fnArgs(args, i))).
      getOrElse(
        throw new AnalysisException(
          s"""Failed to resolve invocation on oracle function ${fnDef.name} on:
             |  ${args.mkString(",")}""".stripMargin)
      )
  }
}

/**
 * Represents a call to a native Oracle function. This is [[Unevaluable]]; so
 * it is valid only for queries where it is operating in an Operator that is
 * rewritten into a [[org.apache.spark.sql.connector.read.oracle.OraScan]]
 *
 * @param fnDef
 * @param sigIdx
 * @param children
 */
case class OraNativeRowFuncInvoke(fnDef : OracleMetadata.OraFuncDef,
                                  sigIdx : Int,
                                  children : Seq[Expression]
                                 )
  extends Expression with UserDefinedExpression with Unevaluable {
  private val overloadFuncDef = fnDef.sigs(sigIdx)

  override def nullable: Boolean = true

  override def dataType: DataType = overloadFuncDef.retType.catalystType
}

case class OraNativeAggFuncInvoke(fnDef : OracleMetadata.OraFuncDef,
                                  sigIdx : Int,
                                  children : Seq[Expression]
                                 )
extends UnevaluableAggregate with Logging
  with UserDefinedExpression {

  assert(fnDef.isAggregate)

  private val overloadFuncDef = fnDef.sigs(sigIdx)

  override def nullable: Boolean = true

  override def dataType: DataType = overloadFuncDef.retType.catalystType
}

/**
 * function actions supported on an [[OracleCatalog]]
 */
trait OraCatalogFunctionActions {self : OracleCatalog =>

  def registerOracleFunction(packageName : Option[String],
                             funcName : String,
                             sparkFuncName : Option[String] = None)(
                              implicit sparkSession: SparkSession
                            ) : String = {

    val oraFuncDef = getMetadataManager.loadFunctionDef(packageName, funcName)
    val fnRegistry = sparkSession.sessionState.functionRegistry
    val fnName = sparkFuncName.getOrElse(funcName)
    val fnId = FunctionIdentifier(fnName, Some(name()))

    fnRegistry.registerFunction(fnId, new OraNativeRowFuncInvokeBuilder(oraFuncDef))

    oraFuncDef.toString

  }

  def registerOracleFunctions(packageName : Option[String],
                              fnSpecs : AnyRef*)(
                               implicit sparkSession: SparkSession
                             ) : String = {

    val funs = fnSpecs.map {
      case nm : String => (nm, None)
      case (nm : String, spkNm : String) => (nm, Some(spkNm))
      case _ => throw new IllegalArgumentException(
        s"Function name must be specified as String or a tuple (Name, SparkName)"
      )
    }

    val sb = new StringBuilder
    for((nm, sNm) <- funs) {
      sb.append(registerOracleFunction(packageName, nm, sNm))
      sb.append("\n")
    }
    sb.toString()
  }
}