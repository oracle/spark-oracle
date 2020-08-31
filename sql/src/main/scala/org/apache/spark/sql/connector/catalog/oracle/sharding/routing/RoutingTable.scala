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

package org.apache.spark.sql.connector.catalog.oracle.sharding.routing

import scala.language.implicitConversions

import oracle.spark.datastructs.{Interval, IntervalTree, QResult, RedBlackIntervalTree}
import oracle.spark.sharding.HashGenerator
import oracle.sql.{Datum, NUMBER}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.oracle.expressions.OraLiterals

trait RoutingKeyRanges { self: RoutingTable =>

  trait RoutingKeyRange extends Interval[RoutingKey]

  case class ConsistentHashRange(start: SingleLevelKey, end: SingleLevelKey)
      extends RoutingKeyRange

  case class MultiLevelKeyRange(gRange: RoutingKeyRange, sRange: RoutingKeyRange)
      extends RoutingKeyRange {

    val start = MultiLevelRKey(gRange.start, sRange.start)
    val end = MultiLevelRKey(gRange.end, sRange.end)

  }

  def createRoutingKeyRange(cInfo: ChunkInfo): RoutingKeyRange = {

    def readHashValue(bytes : Array[Byte]) =
      new NUMBER(java.util.Arrays.copyOfRange(bytes, 1, bytes.length)).longValue()

    val sKeyRange = ConsistentHashRange(
      ChunKey(readHashValue(cInfo.shardKeyLo)),
      ChunKey(readHashValue(cInfo.shardKeyHi))
    )

    tableFamily.superShardType match {
      case NONE_SHARDING => sKeyRange
      case _ =>
        MultiLevelKeyRange(
          ConsistentHashRange(
            ChunKey(readHashValue(cInfo.groupKeyLo)),
            ChunKey(readHashValue(cInfo.groupKeyHi))
          ),
          sKeyRange)
    }
  }

  def createRoutingRange(lowKey: RoutingKey, hiKey: RoutingKey): RoutingKeyRange = {
    null
  }
}

trait RoutingKeys extends RoutingKeyRanges { self: RoutingTable =>

  type ConsistentHash = Long // an unsigned value

  sealed trait RoutingKey extends Ordered[RoutingKey]

  trait SingleLevelKey extends RoutingKey {
    def consistentHash: ConsistentHash

    override def compare(that: RoutingKey): Int = that match {
      case sKey: SingleLevelKey => this.consistentHash.compareTo(sKey.consistentHash)
      case _ => -(that.compareTo(this))
    }
  }

  trait SingleColumnKey extends SingleLevelKey {
    def bytes: Array[Byte]
    val consistentHash: ConsistentHash = RoutingTable.hash(bytes)
  }

  case class OraDatumKey(datum: Datum) extends SingleColumnKey {
    override def bytes: Array[Byte] = datum.getBytes
  }

  case class ChunKey(consistentHash: ConsistentHash) extends SingleLevelKey

  case object MinimumSingleKey extends SingleLevelKey {
    val consistentHash: ConsistentHash = 0L
  }

  case object MaximumSingleKey extends SingleLevelKey {
    val consistentHash: ConsistentHash = 0xffffffffL
  }

  case class MultiColumnRKey(colKeys: Seq[SingleLevelKey]) extends SingleLevelKey {
    val consistentHash: ConsistentHash = colKeys.map(_.consistentHash).sum
  }

  case class MultiLevelRKey(gKey: RoutingKey, sKey: RoutingKey) extends RoutingKey {
    val keys = (gKey, sKey)
    override def compare(that: RoutingKey): Int = {

      that match {
        case mLvlKey: MultiLevelRKey =>
          Ordering[(RoutingKey, RoutingKey)].compare(keys, mLvlKey.keys)
        case _: SingleLevelKey => 1
      }
    }
  }

  implicit def rkeyToLong(rKey: SingleLevelKey): ConsistentHash = rKey.consistentHash

  private def validateLiterals(literals: Seq[Literal]): Unit = {
    if (literals.size != routingColumnDTs.size) {
      OracleMetadata.invalidAction(
        s"Invalid Literals list ${literals.mkString(", ")}",
        None)
    }

    for ((oDT, lit) <- routingColumnDTs.zip(literals)) {
      if (oDT.catalystType != lit.dataType) {
        OracleMetadata.invalidAction(
          s"Invalid Literal ${lit};" +
            s" need literal of type ${oDT.catalystType}",
          None)
      }
    }
  }

  private def multiColumnKey(cols: Seq[SingleLevelKey]): SingleLevelKey = {
    if (cols.size == 1) {
      cols(0)
    } else {
      MultiColumnRKey(cols)
    }
  }

  def createRoutingKey(literals: Literal*): RoutingKey = {
    validateLiterals(literals)

    val gRKeys = literals.zip(gLvlKeyConstructors).map {
      case (oLit, cons) => cons(oLit)
    }

    val sRKeys = literals.drop(gLvlKeyConstructors.size).zip(sLvlKeyConstructors).map {
      case (oLit, cons) => cons(oLit)
    }

    val sKey = multiColumnKey(sRKeys)

    (tableFamily.superShardType) match {
      case NONE_SHARDING => sKey
      case _ => MultiLevelRKey(multiColumnKey(gRKeys), sKey)
    }
  }

  def minMaxKey(endKey: SingleLevelKey): RoutingKey = {
    val sKey = multiColumnKey(Seq.fill(sColumnDTs.size)(endKey))
    (tableFamily.superShardType) match {
      case NONE_SHARDING => sKey
      case _ => MultiLevelRKey(multiColumnKey(Seq.fill(gColumnDTs.size)(endKey)), sKey)
    }
  }

}

trait RoutingQueryInterface { self: RoutingTable =>

  private def resultStreamToShardSet(
      qRes: Stream[QResult[RoutingKey, Array[Int]]]): Set[Int] = {
    val s = scala.collection.mutable.Set[Int]()
    val itr = qRes.iterator
    while (itr.hasNext) {
      val sInstances = itr.next().value
      sInstances.foreach(s.add(_))
    }
    Set[Int](s.toSeq: _*)
  }

  def lookupShardsLT(lits: Literal*): Set[Int] = {
    val rKey = createRoutingKey(lits: _*)
    resultStreamToShardSet(chunkRoutingIntervalTree.lt(rKey))
  }

  def lookupShardsLTE(lits: Literal*): Set[Int] = {
    val rKey = createRoutingKey(lits: _*)
    resultStreamToShardSet(chunkRoutingIntervalTree.lte(rKey))
  }

  def lookupShardsEQ(lits: Literal*): Set[Int] = {
    val rKey = createRoutingKey(lits: _*)
    resultStreamToShardSet(chunkRoutingIntervalTree.contains(rKey))
  }

  def lookupShardsNEQ(lits: Literal*): Set[Int] = {
    val rKey = createRoutingKey(lits: _*)
    resultStreamToShardSet(chunkRoutingIntervalTree.notContains(rKey))
  }

  def lookupShardsGT(lits: Literal*): Set[Int] = {
    val rKey = createRoutingKey(lits: _*)
    resultStreamToShardSet(chunkRoutingIntervalTree.gt(rKey))
  }
  def lookupShardsGTE(lits: Literal*): Set[Int] = {
    val rKey = createRoutingKey(lits: _*)
    resultStreamToShardSet(chunkRoutingIntervalTree.gte(rKey))
  }

  def lookupShardsIN(lits: Array[Array[Literal]]): Set[Int] = {
    val rKeys = lits.map(l => createRoutingKey(l.toSeq: _*))
    resultStreamToShardSet(chunkRoutingIntervalTree.containsAny(rKeys: _*))
  }

  def lookupShardsNOTIN(lits: Array[Array[Literal]]): Set[Int] = {
    throw new UnsupportedOperationException("Shard Pruning for not in predicate")
  }

}

private[sharding] case class RoutingTable private (
    tableFamily: TableFamily,
    rootTable: ShardTable,
    shardCluster: Array[ShardInstance])
    extends RoutingQueryInterface
    with RoutingKeys with Logging {

  val gColumnDTs = rootTable.superKeyColumns.map(_.dataType)
  val sColumnDTs = rootTable.keyColumns.map(_.dataType)
  val routingColumnDTs = gColumnDTs ++ sColumnDTs

  private lazy val shardNameToIdxMap: Map[String, Int] =
    (for ((s, i) <- shardCluster.zipWithIndex) yield {
      (s.name, i)
    }).toMap

  private[routing] lazy val gLvlKeyConstructors: Array[Literal => SingleLevelKey] = {
    val jdbcGetSets = gColumnDTs.map(oDT => OraLiterals.jdbcGetSet(oDT.catalystType))
    for (jGS <- jdbcGetSets) yield { (lit: Literal) =>
      OraDatumKey(jGS.toDatum(lit))
    }
  }

  private[routing] lazy val sLvlKeyConstructors: Array[Literal => SingleLevelKey] = {
    val jdbcGetSets = sColumnDTs.map(oDT => OraLiterals.jdbcGetSet(oDT.catalystType))
    for (jGS <- jdbcGetSets) yield { (lit: Literal) =>
      OraDatumKey(jGS.toDatum(lit))
    }
  }

  private[routing] lazy val minimumRoutingKey: RoutingKey = minMaxKey(MinimumSingleKey)

  private[routing] lazy val maximumRoutingKey: RoutingKey = minMaxKey(MaximumSingleKey)

  private var _chunkRoutingIntervalTree
    : IntervalTree[RoutingKey, Interval[RoutingKey], Array[Int]] = null

  private def initialize(chunks: Array[ChunkInfo]): Unit = {

    val chunkIntervals = chunks.map(c => (createRoutingKeyRange(c), c.shardName))
    logDebug(
      s"""Setting up Routing Table:
         |${chunkIntervals.mkString("\n")}""".stripMargin
    )

    _chunkRoutingIntervalTree = {
      val m: Map[Interval[RoutingKey], Array[Int]] =
        chunkIntervals.groupBy(t => t._1).map {
          case (rrng: RoutingKeyRange, shardNames: Array[(RoutingKeyRange, String)]) =>
            (rrng, shardNames.map(s => shardNameToIdxMap(s._2)))
        }

      RedBlackIntervalTree.create[RoutingKey, Interval[RoutingKey], Array[Int]](m.toIndexedSeq)
    }
  }

  lazy val chunkRoutingIntervalTree: IntervalTree[RoutingKey, Interval[RoutingKey], Array[Int]] =
    _chunkRoutingIntervalTree
}

object RoutingTable {

  def apply(
      tableFamily: TableFamily,
      rootTable: ShardTable,
      shardCluster: Array[ShardInstance],
      chunks: Array[ChunkInfo]): RoutingTable = {
    val rTab = new RoutingTable(tableFamily, rootTable, shardCluster)
    rTab.initialize(chunks)
    rTab
  }

  def hash(var0: Array[Byte]) : Long = {
    Integer.toUnsignedLong(HashGenerator.hash(var0)) % 4294967296L
  }
}
