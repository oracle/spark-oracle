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

import scala.reflect.ClassTag

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryComparison, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, InSet, LessThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.connector.catalog.oracle.sharding.ShardQueryInfo._
import org.apache.spark.sql.connector.catalog.oracle.sharding.routing.RoutingQueryInterface
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.operators.OraTableScan

trait FilterAnnotate { self: AnnotateShardingInfoRule.type =>

  import FilterAnnotate._

  /**
   * - first call `replaceAliases` so we can compare [[AttributeReference]] against
   *   Sharding column names.
   * - check each conjunct, if it is a [[FilterAnnotate.ShardingComparison]]
   * - finally apply [[FilterAnnotate.ShardingComparison]]s to get a pruned shard set.
   *
   * @param from
   * @param filOp
   * @param sparkSession
   * @param shardedMD
   */
  private[sharding] def filter(from: LogicalPlan, filOp: Filter)(
    implicit sparkSession: SparkSession,
    shardedMD: ShardingMetadata): Unit = {
    var sInfo = getShardingQueryInfoOrCoord(from)

    if (sInfo.queryType == ShardedQuery && sInfo.shardTables.size == 1) {
      /*
       * Split info conjuncts
       * extract the conds on the ShardingKeys
       * apply to get ShardingInstance subset
       */

      val conds = replaceAlias(filOp.condition, getAliasMap(from.output))
      val splitConds = splitConjunctivePredicates(conds)
      val shardingFil = ShardingFilters(sInfo.shardTables.head, shardedMD)
      for (c <- splitConds) {
        shardingFil.addCond(c)
      }

      val prunedInstances = shardingFil.pruneShards(sInfo.shardInstances)
      sInfo = sInfo.copy(shardInstances = prunedInstances)
    }

    ShardQueryInfo.setShardingQueryInfo(filOp, sInfo)

  }

  /*
   * If there are filters associated with a OraTableScan
   * then update ShardQueryInfo based on condition.
   */
  private[sharding] def filter(oraTabScan : OraTableScan, ds: DataSourceV2ScanRelation)(
    implicit sparkSession: SparkSession,
    shardedMD: ShardingMetadata): Unit = {
    if (oraTabScan.filter.isDefined || oraTabScan.partitionFilter.isDefined) {
      var filCond : Expression = null
      if (oraTabScan.filter.isDefined) {
        filCond = oraTabScan.filter.get.catalystExpr
      }
      if (oraTabScan.partitionFilter.isDefined) {
        val pCond = oraTabScan.partitionFilter.get.catalystExpr
        if (filCond == null) {
          filCond = pCond
        } else {
          filCond = And(filCond, pCond)
        }
      }
      val fil = Filter(filCond, ds)
      filter(ds, fil)
      for(filSInfo <- ShardQueryInfo.getShardingQueryInfo(fil)) {
        ShardQueryInfo.setShardingQueryInfo(ds, filSInfo)
      }
    }
  }

}

object FilterAnnotate {

  private case class ShardingFilters(shardedTbl: ShardTable, shardedMD: ShardingMetadata) {

    lazy val routingTab: RoutingQueryInterface = shardedMD.getRoutingTable(shardedTbl)

    trait ShardingComparison[V] {
      val lits: Array[V]

      def addLiteral(keyIdx: Int, v: V): Unit =
        lits(keyIdx) = v

      def isApplicable: Boolean = lits.forall(_ != null)

      def getShards: Set[Int]

    }

    private def createLitsArr[V: ClassTag](value: => V = null.asInstanceOf[V]): Array[V] =
      Array.fill[V](shardedTbl.numShardingKeys)(value)

    trait LitComp extends ShardingComparison[Literal] {
      override lazy val lits: Array[Literal] = createLitsArr[Literal]()
    }

    /**
     * Currently only supports single attribute; clauses of the form `attr in (v1, v2, ...)`
     *
     * The `lits` are captured as `Array(Array(v1, v2, ...))`.
     * So position 0 of lits is the values for shardingKey_0.
     *
     * But the `routingTab.lookupShardsIN` and `routingTab.lookupShardsNotIN`
     * expect the literal arrays as an Array of Array of each multi-value.
     * So as Array(Array(v1), Array(v2),...).
     * So we pivot the array before maling the call to routingTab
     */
    trait INComp extends ShardingComparison[Array[Literal]] {
      override lazy val lits: Array[Array[Literal]] = createLitsArr(Array.empty[Literal])
      override def isApplicable: Boolean =
        shardedTbl.numShardingKeys == 1 && lits.forall(_.nonEmpty)

      protected def pivotedLiterals: Array[Array[Literal]] = {
        for (lit <- lits(0)) yield {
          Array(lit)
        }
      }
    }

    val eqComp = new LitComp {
      def getShards: Set[Int] = routingTab.lookupShardsEQ(lits: _*)
    }

    val neqComp = new LitComp {
      def getShards: Set[Int] = routingTab.lookupShardsNEQ(lits: _*)
    }

    val ltComp = new LitComp {
      def getShards: Set[Int] = routingTab.lookupShardsLT(lits: _*)
    }

    val lteComp = new LitComp {
      def getShards: Set[Int] = routingTab.lookupShardsLTE(lits: _*)
    }

    val gtComp = new LitComp {
      def getShards: Set[Int] = routingTab.lookupShardsGT(lits: _*)
    }

    val gteComp = new LitComp {
      def getShards: Set[Int] = routingTab.lookupShardsGTE(lits: _*)
    }

    val inComp = new INComp {
      def getShards: Set[Int] = routingTab.lookupShardsIN(pivotedLiterals)
    }

    val notInComp = new INComp {
      def getShards: Set[Int] = routingTab.lookupShardsNOTIN(pivotedLiterals)
    }

    def comp(op: BinaryComparison): LitComp = op match {
      case _: EqualTo => eqComp
      case _: EqualNullSafe => eqComp
      case _: LessThan => ltComp
      case _: LessThanOrEqual => lteComp
      case _: GreaterThan => gtComp
      case _: GreaterThanOrEqual => gteComp
      case _ =>
        throw new IllegalStateException(
          s"When extracting Sharding predicate from binaruOp expression ${op}")
    }

    def reverseComp(op: BinaryComparison): LitComp = op match {
      case _: EqualTo => eqComp
      case _: EqualNullSafe => eqComp
      case _: LessThan => gtComp
      case _: LessThanOrEqual => gteComp
      case _: GreaterThan => ltComp
      case _: GreaterThanOrEqual => lteComp
      case _ =>
        throw new IllegalStateException(
          s"When extracting Sharding predicate from binaruOp expression ${op}")
    }

    def addComp(aRef: AttributeReference, lit: Literal, litComp: LitComp): Unit = {
      for (sIdx <- shardedTbl.isShardingKey(aRef)) {
        litComp.addLiteral(sIdx, lit)
      }
    }

    def addNotEq(aRef: AttributeReference, lit: Literal): Unit = {
      for (sIdx <- shardedTbl.isShardingKey(aRef)) {
        neqComp.addLiteral(sIdx, lit)
      }
    }

    /**
     * Currently supported for single attribute, so in/not-in
     * clauses of the form `attr in (v1, v2, ...)`
     *
     * TODO: generalize to clauses of the form
     *      `(a,b,..) in ((v1a, v1_b, ..), (v2_a, v2_b, ...), ...)`
     * @param aRef
     * @param lits
     * @param isNot
     */
    def addIn(aRef: AttributeReference, lits: Array[Literal], isNot: Boolean = false): Unit = {
      for (sIdx <- shardedTbl.isShardingKey(aRef) if sIdx == 0) {
        for (lit <- lits) {
          if (isNot) {
            notInComp.addLiteral(sIdx, notInComp.lits(sIdx) :+ lit)
          } else {
            inComp.addLiteral(sIdx, inComp.lits(sIdx) :+ lit)
          }
        }
      }
    }

    def addCond(cond: Expression): Unit = cond match {
      case op @ BinaryComparison(ar: AttributeReference, lit: Literal) =>
        addComp(ar, lit, comp(op))
      case op @ BinaryComparison(lit: Literal, ar: AttributeReference) =>
        addComp(ar, lit, reverseComp(op))
      case Not(op @ BinaryComparison(ar: AttributeReference, lit: Literal))
          if op.isInstanceOf[EqualTo] =>
        addNotEq(ar, lit)
      case Not(op @ BinaryComparison(lit: Literal, ar: AttributeReference))
          if op.isInstanceOf[EqualTo] =>
        addNotEq(ar, lit)
      case In(ar: AttributeReference, lits) =>
        if (lits.forall(_.isInstanceOf[Literal])) {
          addIn(ar, Array[Literal](lits.map(_.asInstanceOf[Literal]): _*))
        }
      case cE @ InSet(ar: AttributeReference, valSet) =>
        val lits = valSet.map(Literal.fromObject(_, cE.child.dataType))
        addIn(ar, lits.toArray)
      case _ => ()
    }

    def pruneShards(input: Set[Int]): Set[Int] = {

      var res = input
      var finish: Boolean = false

      if (eqComp.isApplicable) {
        res = res intersect eqComp.getShards
        finish = true
      }

      if (!finish) {
        var didPrune = false

        for (c <- Seq(ltComp, lteComp, gtComp, gteComp)) {
          if (c.isApplicable) {
            res = res intersect c.getShards
            didPrune = true
          }
        }
        finish = didPrune
      }

      if (!finish && inComp.isApplicable) {
        res = res intersect inComp.getShards
        finish = true
      }

      // maybe also prune on neq, not-in
      // but neq, not-in routing query can be expensive and provides very little pruning.

      res
    }
  }

}
