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

package org.apache.spark.sql.connector.catalog.oracle.sharding

import scala.collection.immutable.TreeSet
import scala.collection.mutable

import oracle.spark.datastructs._
import org.scalacheck.Gen

import org.apache.spark.internal.Logging
import org.apache.spark.sql.oracle.ShardingAbstractTest
import org.apache.spark.sql.oracle.util.TimeIt

// scalastyle:off println
class IntervalTreeTest extends ShardingAbstractTest {
  import IntervalTreeTest._

  test("insert_tests") { td =>

    val testSizes: List[Int] = {
      val seed = org.scalacheck.rng.Seed.random
      val params = Gen.Parameters.default.withInitialSeed(seed)
      Gen.listOfN(numTests, Gen.choose(10, testSz)).apply(params, seed).get
    }

    for (tSz <- testSizes) {
      val l = gen_list(tSz)
      insert_list(l, "list")
      insert_list(l.reverse, "reverse list")
    }
  }

  test("batch_create_tests") {_ =>

    val testSizes: List[Int] = {
      val seed = org.scalacheck.rng.Seed.random
      val params = Gen.Parameters.default.withInitialSeed(seed)
      Gen.listOfN(numTests, Gen.choose(10, testSz)).apply(params, seed).get
    }

    for (tSz <- testSizes) {
      val l = gen_list(tSz)
      batch_create_test(l, "list")
      batch_create_test(l.reverse, "reverse list")
    }
  }

  test("search_tests") { _ =>
    val r = new scala.util.Random()

    for (_ <- (0 until numTests)) {
      val l = gen_list(testSz)
      val tree1 = batch_create(l)
      // val tree1 = create_tree(l)
      timeIt("iterator scan") {
        iterator_test(tree1)
      }
      search_test(tree1, l)(r)
    }
  }

  /*
 * For million intervals:
 *  LTE = 169.27631578947367, 76
    NOT_CONTAINS = 477.79545454545456, 88
    CONTAINS = 0.08139534883720931, 86
    GT = 280.29487179487177, 78
    LT = 184.40963855421685, 83
    GTE = 286.91011235955057, 89
 * Contains is still very fast: < 1 mSec
 * <, <=, >, >= : quite expensive 2-3 hundred mSecs
 * not contains: very expensive, almost 500 mSecs
 *
 * For 100k intervals
 *  LTE = 16.195652173913043, 92
    NOT_CONTAINS = 40.24418604651163, 86
    CONTAINS = 0.07246376811594203, 69
    GT = 24.636363636363637, 77
    LT = 14.164948453608247, 97
    GTE = 25.0, 79
 * Contains is fast: < 1 msec
 * <, <=, >, >= : 20-30 mSecs
 * not contains: closer to 50 mSecs
 *
 * For 10k intervals
 *  LTE = 1.3314917127071824, 181
    NOT_CONTAINS = 3.2666666666666666, 165
    GT = 2.0974025974025974, 154
    CONTAINS = 0.03260869565217391, 184
    GTE = 2.064516129032258, 155
    LT = 1.515527950310559, 161
 * Contains is fast: < 0.5 msec
 * <, <=, >, >= : 1-2 mSecs
 * not contains: closer to 3-5 mSecs
 */
  test("search_perf_tests") {_ =>
    val r = new scala.util.Random()
    val m = mutable.Map[String, (Int, Long)]()
    val l = gen_list(10000)
    val tree = batch_create(l)

    for (i <- (0 until 500)) {
      search_perf_test(tree, r, m)
      print(".")
      if (i > 0 && i % 100 == 0) {
        println("")
      }
    }
    println("")

    for((k, v) <- m) {
      println(s"$k = ${v._2/v._1.toDouble}, ${v._1}")
    }
  }

}

object IntervalTreeTest extends TimeIt with Logging {
  import RedBlackIntervalTree._

  // scalastyle:off println
  def validate(tree : RBTree[_, _, _]) : Unit = {
    tree.validate_clr match {
      case Some(e) =>
        println(s"${e}")
      case _ => ()
    }
  }

  def show(tree : RBTree[_, _, _]) : Unit = {
    println(tree)
  }
  // scalastyle:on

  case class TestInterval(start : Int, end : Int) extends Interval[Int]

  val testSz = 100 * 1000
  val numTests = 25

  def gen_list(sz : Int) : List[Int] = {
    val seed = org.scalacheck.rng.Seed.random
    val params = Gen.Parameters.default.withInitialSeed(seed)
    Gen.listOfN(Math.ceil(sz * 1.3).toInt, Gen.choose(0, sz)).apply(params, seed).get
  }

  def create_tree(l : List[Int]) : RBTree[Int, TestInterval, Int] = {
    var tree : RBTree[Int, TestInterval, Int] = new Empty[Int, TestInterval, Int]
    for (i <- l) {
      tree = tree.insert(TestInterval(i, i + 5), i)
    }
    tree
  }

  def insert_list(l : List[Int],
                  l_name : String) : Unit = {
    val tree = timeIt(s"insert time for ${l_name} of size=${l.size}") {
      create_tree(l)
    }
    validate(tree)
  }

  def batch_create(l : List[Int]) : RBTree[Int, TestInterval, Int] = {
    val lpair = l.toSet[Int] .map(i => (TestInterval(i, i + 5), i))
    RedBlackIntervalTree.create[Int, TestInterval, Int](lpair.toIndexedSeq)
  }

  def batch_create_test(l : List[Int],
                        l_name : String) : Unit = {
    val tree = timeIt(s"batch create time ${l_name} for size=${l.size}") {
      batch_create(l)
    }
    validate(tree)
  }

  def iterator_test(t : RBTree[_, _, Int]) : Unit = {
    val itr = t.iterator
    var lastVal = itr.next().value
    while (itr.hasNext) {
      val nextVal = itr.next().value
      if (lastVal >= nextVal) {
        println(s"Failed iterator test for tree:\n${t}")
      }
      lastVal = nextVal
    }
  }

  def assert_search(
                     t : RBTree[_, _, _],
                     tNm : String,
                     search : () => Stream[QResult[_, _]],
                     res_size : Int
                   ) : Unit = {
    val r = search()
    if (r.size != res_size) {
      val resVals = r.map(_.value)
      println(s"Failed search ${tNm}, res= ${resVals.toList}, expected_sz=${res_size}")
      show(t)
    }
  }

  def search_test(t : RBTree[Int, _, Int],
                  l : List[Int])(r : scala.util.Random) : Unit = {

    val minVal = l.min
    val maxVal = l.max
    val s = TreeSet(l: _*)
    val tSz = t.size

    val randomTestPercent = 10 / tSz.toDouble

    // show(t)

    timeIt("search tests") {

      assert_search(t, s"lt $minVal", () => t.lt(minVal), 0)
      assert_search(t, s"lte $minVal", () => t.lte(minVal), 1)
      assert_search(t, s"gte $minVal", () => t.gte(minVal), tSz)
      assert_search(t, s"gt $minVal", () => t.gt(minVal), tSz - 1)

      assert(t.contains(minVal).size <= 5)


      assert_search(t, s"lt $maxVal", () => t.lt(maxVal), tSz - 1)
      assert_search(t, s"lte $maxVal", () => t.lte(maxVal), tSz)
      assert_search(t, s"gt $maxVal", () => t.gt(maxVal), 0)
      assert_search(t, s"gte $maxVal", () => t.gte(maxVal), 1)

      for (v <- t.iterator.map(_.value)) {
        if (r.nextDouble() < randomTestPercent) {
          val containsSz = t.contains(v).size
          val notContainsSz = t.notContains(v).size
          assert(containsSz + notContainsSz == tSz)
        }

        if (r.nextDouble() < randomTestPercent) {
          val ltSz = t.lt(v).size
          val gteSz = t.gte(v).size
          assert(ltSz + gteSz == tSz)
        }

        if (r.nextDouble() < randomTestPercent) {
          val lteSz = t.lte(v).size
          val gtSz = t.gt(v).size
          assert(lteSz + gtSz == tSz)
        }
      }
    }
  }

  def search_perf_test(t : RBTree[Int, _, Int],
                       r : scala.util.Random,
                       resultMap : mutable.Map[String, (Int, Long)]) : Unit = {

    def doTest[R](a : => R) : Long = {
      val sTime = System.currentTimeMillis()
      a
      val eTime = System.currentTimeMillis()
      eTime - sTime
    }

    val test_type = r.nextInt(6)
    val elem = r.nextInt(t.size)
    type TEST_TYPE = () => Long

    val (testNm : String, test : TEST_TYPE) = test_type match {
      case 0 => ("LT", () => doTest[List[_]](t.lt(elem).toList))
      case 1 => ("LTE", () => doTest[List[_]](t.lte(elem).toList))
      case 2 => ("GT", () => doTest[List[_]](t.gt(elem).toList))
      case 3 => ("GTE", () => doTest[List[_]](t.gte(elem).toList))
      case 4 => ("CONTAINS", () => doTest[List[_]](t.contains(elem).toList))
      case 5 => ("NOT_CONTAINS", () => doTest[List[_]](t.notContains(elem).toList))
      case _ => ???
    }

    val curr = resultMap.get(testNm)
    val testTime = test()
    resultMap(testNm) = curr.map(r => (r._1 + 1, r._2 + testTime)).getOrElse((1, testTime))
  }
}
