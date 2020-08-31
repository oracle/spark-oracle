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
import java.util.concurrent.{ConcurrentHashMap => CMap}

import oracle.spark.ORAMetadataSQLs

import org.apache.spark.sql.oracle.OraSparkConfig

trait OraTypes { self: OracleMetadataManager =>

  private val REGEX_QUAL_TYP_NAME = "(.*?)\\.(.*)".r

  private case class TypeXMLParser(sxml: String) {
    import scala.xml.NodeSeq
    import scala.xml.XML._
    import XMLParsingUtils._

    case class DataTypeElem(nd : NodeSeq) {
      val typName = textValue(nd \ "DATATYPE").get
      val length = intValue(nd \ "LENGTH")
      val typProps = exists(nd \ "TYPE_PROPERTIES")
      val schema = if (typProps) textValue(nd \ "TYPE_PROPERTIES" \ "SCHEMA") else None
      val udtTypNm =
        if (typProps) textValue(nd \ "TYPE_PROPERTIES" \ "NAME") else None

      val oraDT = if (typName.toUpperCase() == "UDT") {
        createOraDataType(s"${schema.get}.${udtTypNm.get}", None, None, None)
      } else {
        createOraDataType(typName, length, None, None)
      }
    }

    case class AttrElem(nd : NodeSeq) {
      val name = textValue(nd \ NAME_TAG).get
      val dataTypeElem = DataTypeElem(nd)
      val field = OraStructField(name, dataTypeElem.oraDT)
    }

    case class ObjTypElem(nd : NodeSeq) {
      val attrs = {
        val attrItems = nd \ "ATTRIBUTE_LIST" \ "ATTRIBUTE_LIST_ITEM"
        attrItems.map(AttrElem).toArray
      }
      val fieldDTs = attrs.map(_.field)
    }

    case class NestedTableTypElem(nd : NodeSeq) {
      val elemTyp = DataTypeElem(nd)
    }

    case class VarrayTypElem(nd : NodeSeq) {
      val elemTyp = DataTypeElem(nd)
      val limit = intValue(nd \ "LIMIT").get
    }

    val nd = loadString(sxml)
    val schema = (nd \ SCHEMA_TAG).text
    val typName = (nd \ NAME_TAG).text


    def parseType: OraDataType = {
      if (!(nd \ "OBJECT").isEmpty) {
        val objElem = ObjTypElem(nd \ "OBJECT")
        OraStructType(schema, typName, objElem.fieldDTs)
      } else if (!(nd \ "NESTED_TABLE").isEmpty) {
        val nestTabElem = NestedTableTypElem(nd \ "NESTED_TABLE")
        OraNestedTableType(schema, typName, nestTabElem.elemTyp.oraDT)
      } else if (!(nd \ "VARRAY").isEmpty) {
        val varrayElem = VarrayTypElem(nd \ "VARRAY")
        OraArrayType(schema, typName, varrayElem.elemTyp.oraDT, varrayElem.limit)
      } else {
        OracleMetadata.unsupportedAction(
          s"Unsupported Kind of Type, currently we only support OBJECT, NESTED_TABLE and VARRAY"
        )
      }
    }
  }

  private[oracle] val userDefinedTypes = new CMap[String, OraDataType]()

  private def typeString(
      s: String,
      length: Option[Int],
      precision: Option[Int],
      scale: Option[Int]): String = {
    var r: String = s

    val pDefined = precision.isDefined
    val sDefined = scale.isDefined
    val lDefined = length.isDefined
    val addBrackets = pDefined || sDefined
    val addComma = pDefined && sDefined

    def add(cond: Boolean, s: => String): Unit = {
      if (cond) {
        r += s
      }
    }

    add(addBrackets, "(")
    add(pDefined, precision.get.toString)
    add(addComma, ", ")
    add(sDefined, scale.get.toString)
    add(addBrackets, ")")
    add(lDefined, length.get.toString)

    r
  }

  private[oracle] def createOraDataType(
      s: String,
      length: Option[Int],
      precision: Option[Int],
      scale: Option[Int]): OraDataType =
    (s.toUpperCase(Locale.ROOT), length, precision, scale) match {
      case ("CHAR", Some(l), None, None) => OraChar(l, true)
      case ("VARCHAR2", Some(l), None, None) => OraVarchar2(l, true)
      case ("VARCHAR2", None, None, None) =>
        OraVarchar2(OraSparkConfig.getConf(OraSparkConfig.VARCHAR2_MAX_LENGTH), true)
      case ("NCHAR", Some(l), None, None) => OraNChar(l)
      case ("NVARCHAR2", Some(l), None, None) => OraNVarchar2(l)
      case ("NVARCHAR2", None, None, None) =>
        OraNVarchar2(OraSparkConfig.getConf(OraSparkConfig.VARCHAR2_MAX_LENGTH))
      case ("NUMBER", _, p, s) =>
        OraNumber.checkSupported(p, s)
        OraNumber(p, s)
      case ("FLOAT", None, p, None) => OraFloat(p)
      case ("LONG", None, None, None) => OraLong
      case ("BINARY_INTEGER", None, None, None) => OraNumber(Some(9), None)
      case ("BINARY_FLOAT", None, None, None) => OraBinaryFloat
      case ("BINARY_DOUBLE", None, None, None) => OraBinaryDouble
      case ("DATE", None, None, None) => OraDate
      // case ("TIMESTAMP", p, None) => OraTimestamp(p)
      case ("TIMESTAMP", None, None, s) => OraTimestamp(s)
      // TODO TIMESTAMP WITH TZ, TIMESTAMP WITh LOCAL TZ
      case (REGEX_QUAL_TYP_NAME(schema, typNm), _, _, _) => loadType(schema, typNm)
      case _ => OracleMetadata.unsupportedOraDataType(typeString(s, length, precision, scale))
    }

  private[oracle] def loadType(schema: String, typName: String): OraDataType = {
    val qualNm = s"${schema}.${typName}"

    val loadAndParse = new java.util.function.Function[String, OraDataType] {
      override def apply(qualTypNm: String): OraDataType = {
        val sxml = ORAMetadataSQLs.typeMetadata(dsKey, schema, typName)
        TypeXMLParser(sxml).parseType
      }
    }
    userDefinedTypes.computeIfAbsent(qualNm, loadAndParse)
  }
}

trait OraCatalogTypesActions { self: OracleCatalog =>

  def registerOraType(schema: String, typName: String) : Unit = {
    getMetadataManager.loadType(schema, typName)
  }
}