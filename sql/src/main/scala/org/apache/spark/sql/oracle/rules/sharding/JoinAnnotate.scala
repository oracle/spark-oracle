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

package org.apache.spark.sql.oracle.rules.sharding

import scala.collection.mutable.{Map => MMap, Set => MSet}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, NaturalJoin, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.connector.catalog.oracle.sharding.ShardQueryInfo._
import org.apache.spark.sql.connector.read.oracle.{OraFileScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.oracle.rules.NotInJoinPattern

/**
 * ''Shard Preserving Rules:''
 *  - For non-equi Joins
 *    - For non outer joins:
 *      - if both sides replicated => result [[ReplicatedQuery]]
 *      - if one side is sharded, other is replicated => res has sharded sides [[ShardQueryInfo]]
 *      - otherwise => result is [[CoordinatorQuery]]
 *    - For outer joins:
 *      - Same logic as inner joins
 *      - This works because one of the sides has Replicated tables only.
 *        So pushing a 'Shard Table join Replicated Table' to shard instances is ok.
 *  - For equi Joins
 *   - For Inner, LeftSemi and Outer joins
 *     - analyze the joining keys
 *     - find a pair of ShardTables one from the lhs and the other from the rhs
 *       such that Seq of (leftKeys, rightKeys) contains equality condition for all the
 *       sharding key columns in the ShardTables.
 *     - If such a pair is found, join is sharding preserving.
 *       - res:
 *         - for inner, leftSemi, leftAnti: shardInstances is intersect of 2 sides
 *         - for leftOuter: sInfoLeft.shardInstances
 *         - for rightOuter: sInfoRight.shardInstances
 *         - for fullOuter: sInfoLeft.shardInstances union sInfoRight.shardInstances
 *   - For LeftAnti
 *     - logic is same as above, but look for Equality condition in join condition using
 *       [[org.apache.spark.sql.oracle.rules.NotInJoinPattern]]
 */
trait JoinAnnotate {
  self: AnnotateShardingInfoRule.type =>

  import JoinAnnotate._


  private[sharding] def nonEquiJoin(joinOp: Join)(
    implicit sparkSession: SparkSession, shardedMD: ShardingMetadata)
  : Unit = analyzeNonEquiJoinForSharding(joinOp)


  private[sharding] def equiJoin(joinOp: Join,
                                 leftKeys: Seq[Expression],
                                 rightKeys: Seq[Expression])(
    implicit sparkSession: SparkSession, shardedMD: ShardingMetadata): Unit = {
    analyzeEquiJoinForSharding(joinOp, leftKeys, rightKeys)
  }

  private[sharding] def nonInJoin(joinOp: Join,
                                  leftKeys: Seq[Expression],
                                  rightKeys: Seq[Expression],
                                  joinCond: Option[Expression])(
    implicit sparkSession: SparkSession, shardedMD: ShardingMetadata)
  : Unit = {
    analyzeEquiJoinForSharding(joinOp, leftKeys, rightKeys, {
      val notInConjuncts = joinCond.map(splitConjunctivePredicates).getOrElse(Seq.empty)
      val notInJoinPattern = NotInJoinPattern(joinOp)
      OraSparkUtils.sequence(notInConjuncts.map(notInJoinPattern.unapply(_)))
    })
  }

}

object JoinAnnotate extends AnnotatePredicateHelper {

  /**
   * Captures joinTypes we currently analyze for preserving sharding.
   *
   * @param joinType
   * @return
   */
  private def canHandle(joinType : JoinType) : Boolean = joinType match {
    case _ : NaturalJoin | _ : ExistenceJoin |  _ : UsingJoin => false
    case _ => true
  }

  /**
   * Sharding preserving is based on [[ShardQueryInfo]] of the 2 sides and the joinType
   *
   * If both sides Replicated => result is Replicated
   * If one side Sharded => result is Sharded
   * Otherwise CoordQuery
   *
   * @param left
   * @param right
   * @param joinOp
   * @param sparkSession
   * @param shardedMD
   */
  private def analyzeNonEquiJoinForSharding(joinOp: Join)
                              (implicit sparkSession: SparkSession, shardedMD: ShardingMetadata)
  : Unit = {
    val left = joinOp.left
    val right = joinOp.right
    val sInfoLeft = getShardingQueryInfoOrCoord(left)
    val sInfoRight = getShardingQueryInfoOrCoord(right)

    val sResQInfo = if (canHandle(joinOp.joinType)) {
      (sInfoLeft.queryType, sInfoRight.queryType) match {
        case (ReplicatedQuery, ReplicatedQuery) =>
          shardedMD.REPLICATED_TABLE_INFO
        case (ShardedQuery, ReplicatedQuery) =>
          sInfoLeft
        case (ReplicatedQuery, ShardedQuery) =>
          sInfoRight
        case _ => shardedMD.COORD_QUERY_INFO
      }
    } else {
      shardedMD.COORD_QUERY_INFO
    }

    ShardQueryInfo.setShardingQueryInfo(joinOp, sResQInfo)
  }

  private def analyzeEquiJoinForSharding(joinOp: Join,
                                         leftKeys: Seq[Expression],
                                         rightKeys: Seq[Expression],
                                         notInKeys : Option[Seq[(Expression, Expression)]] = None
                                    )(
    implicit sparkSession: SparkSession, shardedMD: ShardingMetadata
  ) : Unit = {
    val left = joinOp.left
    val right = joinOp.right
    val sInfoLeft = getShardingQueryInfoOrCoord(left)
    val sInfoRight = getShardingQueryInfoOrCoord(right)

    /*
     * If both sides are on the same tableFamily
     * - check if join is on shardingKeys
     */

    if (sInfoLeft.queryType == ShardedQuery &&
      sInfoRight.queryType == ShardedQuery &&
      sInfoLeft.tableFamily == sInfoRight.tableFamily
    ) {

      val (notInLKeys : Seq[Expression], notInRKeys : Seq[Expression]) =
        notInKeys.map(_.unzip).getOrElse(Seq.empty, Seq.empty)

      val sJInfo : ShardingJoinInfo = {
        val sJInfo = ShardingJoinInfo(
          leftKeys ++ notInLKeys,
          rightKeys ++ notInRKeys,
          joinOp,
          shardedMD)
        sJInfo.scanJoinKeys
        sJInfo
      }

      if (sJInfo.isShardPreserving) {
        val shardInstances = joinOp.joinType match {
          case LeftOuter => sInfoLeft.shardInstances
          case RightOuter => sInfoRight.shardInstances
          case FullOuter => sInfoLeft.shardInstances union sInfoRight.shardInstances
          case Inner | LeftSemi => sInfoLeft.shardInstances intersect sInfoRight.shardInstances
          // for leftAnti intersect is valid because
          // for a left row that doesn't have a matching row in the same shard,
          //   there will not exist one in the other shards.
          case LeftAnti => sInfoLeft.shardInstances intersect sInfoRight.shardInstances
          // this case should not be reached.
          case _ => sInfoLeft.shardInstances union sInfoRight.shardInstances
        }

        val res = ShardQueryInfo(
          ShardedQuery,
          sInfoLeft.shardTables ++ sInfoRight.shardTables,
          shardInstances,
          None
        )
        ShardQueryInfo.setShardingQueryInfo(joinOp, res)
      } else {
        ShardQueryInfo.setShardingQueryInfo(joinOp, shardedMD.COORD_QUERY_INFO)
      }

    } else {
      analyzeNonEquiJoinForSharding(joinOp)
    }

  }

  case class ShardingJoinInfo(leftKeys: Seq[Expression],
                              rightKeys: Seq[Expression],
                              joinOp: Join,
                              shardedMD: ShardingMetadata) {
    type ShardJoinId = ((OpCode, QualifiedTableName), (OpCode, QualifiedTableName))

    val shardJoins = MMap[ShardJoinId, MSet[Int]]()


    def extractFrom(e: Expression): Option[(OpCode, ShardTable, Int)] = toShardingKey(e, joinOp)

    def scanJoinKeys : Unit = {
      for ((lKey, rKey) <- leftKeys.zip(rightKeys);
           (lOpCode, lShardTable, lShardColIdx) <- extractFrom(lKey);
           (rOpCode, rShardTable, rShardColIdx) <- extractFrom(rKey)
           if (lShardColIdx == rShardColIdx) // join must be on corresponding sharding key cols
           ) {
        val shardJoinId = ((lOpCode, lShardTable.qName), (rOpCode, rShardTable.qName))
        val joins = shardJoins.getOrElseUpdate(shardJoinId, MSet[Int]())
        joins += lShardColIdx
      }
    }

    /**
     * Can this join be done on Shard Instances?
     * @return
     */
    def isShardPreserving : Boolean = {

      /*
       * just need to check if set of shardKeyIdxes has size equal to numShardingKeys
       * This implies all corresponding sharding keys are equi-joined.
       */
      def isCompleteJoin(lTable : ShardTable,
                         rTable : ShardTable,
                         shardKeyIdxes : MSet[Int]) =
        lTable.numShardingKeys == rTable.numShardingKeys &&
          lTable.numShardingKeys == shardKeyIdxes.size


      val joinOfShardTables = shardJoins.toIterator.find {
        case (((_, lTblNm), (_, rTblNm)), shardKeyIdxes) =>
          val lTbl = shardedMD.getShardTable(lTblNm)
          val rTbl = shardedMD.getShardTable(rTblNm)
          isCompleteJoin(lTbl, rTbl, shardKeyIdxes)
      }
      joinOfShardTables.isDefined
    }
  }

}
