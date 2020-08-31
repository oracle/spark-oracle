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
package org.apache.spark.sql.oracle.querysplit

import scala.collection.mutable.{ArrayBuffer, Map => MMap, Stack}
import scala.util.Try

import oracle.spark.{DataSourceKey, ORAMetadataSQLs}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.oracle.OraSparkUtils.sequence
import org.apache.spark.sql.oracle.expressions.OraLiterals
import org.apache.spark.sql.oracle.operators.OraPlan

/**
 * Invoke [[ORAMetadataSQLs#queryPlan]] to get the oracle plan in xml form.
 * Extract overall plan stats and when `extractTabAccess = true` extract
 * ''Table Access Operations'' and build [[TableAccessOperation]] structs.
 * Return the [[PlanInfo]] for given [[OraPlan]]
 */
object OraExplainPlan extends Logging {

  import scala.xml.{Node, NodeSeq}
  import scala.xml.XML._

  private[oracle] def oraExplainPlanXML(dsKey: DataSourceKey, oraPlan: OraPlan): String = {
    val oSQL = oraPlan.orasql
    ORAMetadataSQLs.queryPlan(dsKey, oSQL.sql, ps => {
      OraLiterals.bindValues(ps, oSQL.params)
    })
  }

  def constructPlanInfo(dsKey: DataSourceKey,
                        oraPlan: OraPlan,
                        extractTabAccess: Boolean,
                        extractShardCosts : Boolean = false): Option[PlanInfo] = {
    val pO = PlanXMLReader.parsePlan(
      oraExplainPlanXML(dsKey, oraPlan),
      extractTabAccess,
      extractShardCosts
    )

    if (!pO.isDefined) {
      logWarning(
        s"""Failed to construct PlanInfo for generated sql:
           |${oraPlan.orasql.sql}
           |""".stripMargin
      )
    }
    pO
  }

  private object PlanXMLReader {

    def textValue(nd: NodeSeq): Option[String] = {
      if (nd.nonEmpty) Some(nd.text) else None
    }

    def intValue(nd: NodeSeq): Option[Int] = {
      textValue(nd).flatMap(v => Try { v.toInt }.toOption)
    }

    def longValue(nd: NodeSeq): Option[Long] = {
      textValue(nd).flatMap(v => Try { v.toLong }.toOption)
    }

    def doubleValue(nd: NodeSeq): Option[Double] = {
      textValue(nd).flatMap(v => Try { v.toDouble }.toOption)
    }

    private def attrVal[T](nd: Node,
                attrNm: String,
                parseVal : NodeSeq => Option[T]): Option[T] = {
      for (aNd <- nd.attribute(attrNm) if aNd.size == 1;
           tV <- parseVal(aNd.head)
           ) yield tV
    }

    private val TIME_ATTR_REGEX = raw"(?:(\d{2}):)?(\d{2}):(\d{2})".r
    private def timeVal(s : Option[String]) : Option[Long] = {

      if (s.isDefined) {

        val MIN_SECS = 60
        val HOUR_SECS = MIN_SECS * 60

        s.get.trim match {
          case TIME_ATTR_REGEX(null, min, sec) => Some(min.toLong * MIN_SECS + sec.toLong)
          case TIME_ATTR_REGEX(hr, min, sec) =>
            Some(hr.toLong * HOUR_SECS + min.toLong * MIN_SECS + sec.toLong)
          case _ => None
        }
      } else None
    }

    case class OpCost(row_count: Long, bytes : Long)

    def operations(root: NodeSeq): NodeSeq = root \ "plan" \ "operation"

    private def nodeIdDepth(nd: Node): Option[(Int, Int, String)] = {
      for (
        id <- attrVal[Int](nd, "id", intValue _);
        depth <- attrVal[Int](nd, "depth", intValue _);
        nm <- attrVal[String](nd, "name", textValue _)
      ) yield {
        (id, depth, nm)
      }
    }

    def opCost(nd: Node): Option[OpCost] = {

      for (row_count <- longValue(nd \ "card");
           bytes <- longValue(nd \ "bytes")
           ) yield {
        OpCost(row_count, bytes)
      }
    }

    def operation(nd: Node): Option[TableAccessOperation] = {
      for (tbNm <- textValue(nd \ "object");
           qBlk <- textValue(nd \ "qblock");
           obj_alias <- textValue(nd \ "object_alias");
           row_count <- longValue(nd \ "card");
           bytes <- longValue(nd \ "bytes");
           partition = nd \ "partition") yield {

        val pRng : Option[(Int, Int)] = if (partition.nonEmpty) {
          for (startS <- partition.head.attribute("start");
               start <- intValue(NodeSeq.fromSeq(startS));
               endS <- partition.head.attribute("stop");
               end <- intValue(NodeSeq.fromSeq(endS))) yield {
            (start, end)
          }
        } else None

        val alias = obj_alias.replaceAll("\\\"", "").split("@").head
        TableAccessOperation(tbNm, qBlk, obj_alias, alias, row_count, bytes, pRng)
      }
    }

    def tableAccessOperations(nd : NodeSeq) : Option[Seq[TableAccessOperation]] = {
      sequence(
      nd.filter { nd =>
        val nmS = nd.attribute("name")
        val nm = nmS.flatMap(s => textValue(NodeSeq.fromSeq(s)))
        nm.isDefined && nm.get == "TABLE ACCESS"
      }.map(operation)
      )
    }

    def shardWorkersInfo(root : Node) : Option[ShardingPlanInfo] = {

      val idPrntIdMap = MMap[Int, Int]()
      val idNodeMap = MMap[Int, Node]()
      val remoteNodes = ArrayBuffer[Int]()
      var hasJoins : Boolean = false
      var hasTableAccess : Boolean = false

      def buildOpRelMap(ops : NodeSeq) : Unit = {
        val stack = Stack[(Int, Int)]()

        for(nd <- ops) {
          val ndIdDepthPos : Option[(Int, Int, String)] = nodeIdDepth(nd)
          if (ndIdDepthPos.isDefined) {
            val Some((id : Int, depth : Int, nm : String)) = ndIdDepthPos
            while (stack.nonEmpty && stack.top._2 >= depth) stack.pop()
            if (stack.nonEmpty) {
              idPrntIdMap(id) = stack.top._1
            }
            idNodeMap(id) = nd
            stack.push((id, depth))

            nm match {
              case "REMOTE" => remoteNodes += id
              case "TABLE ACCESS" => hasTableAccess = true
              case "INDEX" => hasTableAccess = true
              case n if n.contains("JOIN") => hasJoins = true
              case _ => ()
            }
          }
        }
      }

      def _coordCostTime(ndId : Int) : (Option[Long], Option[Long]) = {
        def coordNd(nd : Node) : (Boolean, Option[Long], Option[Long]) = {
          val shardCordNode : Boolean =
            (for (obj <- textValue(nd \ "object")) yield {
              obj.startsWith("VW_SHARD_")
            }).getOrElse(false)

          if (shardCordNode) {
            (true, longValue(nd \ "cost"), timeVal(textValue(nd \ "time")))
          } else {
            (false, None, None)
          }
        }

        val (isCordNd, cordCost, cordTime) = coordNd(idNodeMap(ndId))
        if (isCordNd) {
          (cordCost, cordTime)
        } else {
          idPrntIdMap.get(ndId).map(_coordCostTime).getOrElse((None, None))
        }
      }

      val ops = operations(root)
      buildOpRelMap(ops)

      val (shardCosts, shardTimes) = remoteNodes.map(id => _coordCostTime(id)).unzip

      for(
      costs <- OraSparkUtils.sequence(shardCosts) if costs.nonEmpty;
      times <- OraSparkUtils.sequence(shardTimes) if times.nonEmpty;
      rootOpCost <- longValue(ops.head \ "cost");
      rootOpTime <- timeVal(textValue(ops.head \ "time"))
      ) yield {
        ShardingPlanInfo(
          costs.sum, times.sum, costs.size,
          rootOpCost, rootOpTime, hasJoins, hasTableAccess
        )
      }

    }

    def parsePlan(xml: String,
                  extractTabAccess: Boolean,
                  extractShardCosts : Boolean): Option[PlanInfo] = {

      val root = loadString(xml)
      val ops = operations(root)

      for(
        rootOpCost <- opCost(ops.head);
        tblOps <- if (extractTabAccess) tableAccessOperations(ops) else Some(Seq.empty)
      ) yield {
        PlanInfo(
          rootOpCost.row_count,
          rootOpCost.bytes,
          tblOps,
          if (extractShardCosts) shardWorkersInfo(root) else None
        )
      }
    }
  }
}
