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

package oracle.spark.datastructs

import scala.language.implicitConversions

/**
 * An interval for type some `E` that is `Orderable`
 * @param ordering$E$0
 * @tparam E
 */
abstract class Interval[E: Ordering] extends Ordered[Interval[E]] {
  val o = Ordering[E]
  import o._

  def start: E
  def end: E

  def contains(value: E): Boolean = {
    start <= value && value < end
  }

  override def compare(that: Interval[E]): Int = {
    val c = o.compare(this.start, that.start)
    if (c == 0) {
      o.compare(this.end, that.end)
    } else c
  }
}

trait QResult[E, V] {
  def interval : Interval[E]
  def value : V
}

/**
 * An interval tree of `I <: Interval[E]` for some orderable type `E`.
 * Associate with each `I` is a value of type `V`.
 * The search interface provides methods to return `QResult[I, V]` values
 * that are `lt` or `lte` or `gt` or `gte` or `contains` or `notContains` some `E` value.
 *
 * @param ordering$E$0
 * @tparam E
 * @tparam I
 * @tparam V
 */
abstract class IntervalTree[E: Ordering, I <: Interval[E], V] extends Iterable[QResult[E, V]] {


  val o = Ordering[E]
  import o._

  /**
   * return all intervals that come before 'e'
   * @param e
   * @return
   */
  def lt(e: E): Stream[QResult[E, V]]

  /**
   * return all intervals that contain or come before 'e'
   * @param e
   * @return
   */
  def lte(e: E): Stream[QResult[E, V]]

  /**
   * return all intervals that contain 'e'
   * @param e
   * @return
   */
  def contains(e: E): Stream[QResult[E, V]]

  /**
   * return all intervals that don't contain 'e'
   * @param e
   * @return
   */
  def notContains(e: E): Stream[QResult[E, V]]

  /**
   * return all intervals that come after 'e'
   * @param e
   * @return
   */
  def gt(e: E): Stream[QResult[E, V]]

  /**
   * return all intervals that contain or come after 'e'
   * @param e
   * @return
   */
  def gte(e: E): Stream[QResult[E, V]]

  def containsAny(e: E*): Stream[QResult[E, V]] = {

    def merge(s1 : Stream[QResult[E, V]], s2 : Stream[QResult[E, V]]) : Stream[QResult[E, V]]
    = (s1, s2) match {
      case (s1, s2) if s1.isEmpty => s2
      case (s1, s2) if s2.isEmpty => s1
      case (h1 #:: t1, h2 #:: t2) =>
        val c = compare(h1.interval.start, h2.interval.start)
        if ( c < 0) {
          h1 #:: merge(t1, s2)
        } else if ( c > 0) {
          h2 #:: merge(t2, s1)
        } else {
          h1 #:: merge(t1, t2)
        }
    }

    val eStrms = e.map(contains)

    eStrms.foldLeft(Stream.empty[QResult[E, V]])(merge)
  }

}

/**
 * Implementation of [[IntervalTree]] based on Osasaki Red Black Trees data structure.
 */
object RedBlackIntervalTree {

  sealed trait Color
  case object Red extends Color
  case object Black extends Color


  abstract class RBTree[E: Ordering, I <: Interval[E], V] extends IntervalTree[E, I, V]
  with Iterable[QResult[E, V]] {

    object NodePattern {
      def unapply(t : RBNode[E, I, V]) :
      Option[(Color, RBTree[E, I, V], RBTree[E, I, V])] = t match {
        case rNd : RBNode[E, I, V] => Some((rNd.c, rNd.l, rNd.r))
      }
    }

    def empty : RBTree[E, I, V] = new Empty

    private def span(i : I, child : RBTree[E, I, V]) : (E, E) =
      int_span[E, I, V](i, child)

    // scalastyle:off line.size.limit
    private def balance(t : RBNode[E, I, V]) : RBNode[E, I, V] = t match {
      case z@NodePattern(Black, y@NodePattern(Red, x@NodePattern(Red, _, _), c), d) =>
        val newR = new RBNode(Black, z.interval, z.value, span(z.interval, c), c, span(z.interval, d), d)(o)
        new RBNode(
          Red,
          y.interval,
          y.value,
          span(y.interval, x),
          x.copy(c = Black),
          span(y.interval, newR),
          newR
        )(o)
      case z@NodePattern(Black, x@NodePattern(Red, a, y@NodePattern(Red, b, c)), d ) =>
        val newL = new RBNode(Black, x.interval, x.value, span(x.interval, a), a, span(x.interval, b), b)(o)
        val newR = new RBNode(Black, z.interval, z.value, span(z.interval, c), c, span(z.interval, d), d)(o)
        new RBNode(
          Red,
          y.interval,
          y.value,
          span(y.interval, newL),
          newL,
          span(y.interval, newR),
          newR
        )(o)
      case x@NodePattern(Black, a, y@NodePattern(Red, b, z@NodePattern(Red, _, _))) =>
        val newL = new RBNode(Black, x.interval, x.value, span(x.interval, a), a, span(x.interval, b), b)(o)
        new RBNode(
          Red,
          y.interval,
          y.value,
          span(y.interval, newL),
          newL,
          span(y.interval, z),
          z.copy(c = Black)
        )(o)
      case x@NodePattern(Black, a, z@NodePattern(Red, y@NodePattern(Red, b, c), d)) =>
        val newL = new RBNode(Black, x.interval, x.value, span(x.interval, a), a, span(x.interval, b), b)(o)
        val newR = new RBNode(Black, z.interval, z.value, span(z.interval, c), c, span(z.interval, d), d)(o)
        new RBNode(
          Red,
          y.interval,
          y.value,
          span(y.interval, newL),
          newL,
          span(y.interval, newR),
          newR
        )(o)
      case t =>
        val (minL, maxL) = span(t.interval, t.l)
        val (minR, maxR) = span(t.interval, t.r)
        t.copy(minL = minL, maxL = maxL, minR = minR, maxR = maxR)
    }
    // scalastyle:on

    def insert(i : I, v : V) : RBTree[E, I, V] = {

      def ins(t : RBTree[E, I, V]) : RBNode[E, I, V] = t match {
        case e : Empty[E, I, V] => RBNode(Red, i, i.start, i.start, i.end, i.end, v, e, e)
        case s : RBNode[E, I, V] => if (i < s.interval) {
          t.balance(s.copy(l = ins(s.l)))
        } else if ( i > s.interval) {
          t.balance(s.copy(r = ins(s.r)))
        } else s
      }
      ins(this).copy(c = Black)
    }

    private def black_height_clr : Int = this match {
      case _ : Empty[E, I, V] => 0
      case s : RBNode[E, I, V] if s.c == Red => s.l.black_height_clr + 1
      case s : RBNode[E, I, V] =>
        val cBH = s.l.black_height_clr
        val isCBlk = (s.l, s.r) match {
          case (sL : RBNode[E, I, V], _) if sL.c == Black => 1
          case (_, sR : RBNode[E, I, V]) if sR.c == Black => 1
          case _ => 0
        }
        cBH + isCBlk
    }

    private def black_height : Int = this match {
      case _ : Empty[E, I, V] => 0
      case s : RBNode[E, I, V] if s.c == Red => s.l.black_height
      case s : RBNode[E, I, V] => s.l.black_height + 1
    }

    private def height_clr : Int = this match {
      case e : Empty[E, I, V] => 0
      case s : RBNode[E, I, V] =>
        (s.l, s.r) match {
          case (_ : Empty[E, I, V], _ : Empty[E, I, V]) => 0
          case _ => 1 + Math.max(s.l.height_clr, s.r.height_clr)
        }
    }

    override def size : Int = this match {
      case _ : Empty[E, I, V] => 0
      case s : RBNode[E, I, V] => 1 + s.l.size + s.r.size
    }

    /*
     * There is a bug in our insert
     * - there are cases where height > 2 * Math.log(n + 1)
     * - running 1000s of tests on size=1mil;
     *   there are cases where height = 2 * Math.log(n + 1) + [1 or 2]
     *
     * Not yet able to find the issue. Our implementation is based on Okasaki's algorithm.
     * Since difference is [1 or 2] deferring for now since we wil only be using the
     * `RedBlackTree.create` batch create function.
     */
    def validate_clr : Option[String] = {
      val blackHeightCheck = this match {
        case _ : Empty[E, I, V] => None
        case s : RBNode[E, I, V] =>
          if (s.l.black_height != s.r.black_height) {
            Some(s"invalid tree(children black_heights don't match)")
          } else None
      }

      blackHeightCheck.orElse {
        // check height
        val n = size
        val h = height_clr
        if (h > 2 * Math.log(n + 1)) {
          Some(s"invalid tree(height=${h}, size=${n}," +
            s" height is > 2 * Math.log(n + 1)(${2 * Math.log(n + 1)})")
        } else None
      }
    }

    def dump(buf: StringBuilder)(ident : Int = 0,
                                 prefix : String = ""): Unit = this match {
      case e : Empty[E, I, V] => () // buf.append(s"${" " * ident} Empty\n")
      case s : RBNode[E, I, V] =>
        buf.append(s"${" " * ident}${prefix} color=${s.c}, interval= ${s.interval}, " +
          s"leftspan= ${s.minL}, ${s.maxL}, rightspan= ${s.minR}, ${s.maxR}\n")
        s.l.dump(buf)(ident + 1, "[L]")
        s.r.dump(buf)(ident + 1, "[R]")
    }

    override def toString: String = {
      val sb = new StringBuilder
      dump(sb)(0)
      sb.toString()
    }
  }

  class Empty[E: Ordering, I <: Interval[E], V] extends RBTree[E, I, V] {
    override def lt(e: E): Stream[QResult[E, V]] = Stream.empty

    override def lte(e: E): Stream[QResult[E, V]] = Stream.empty

    override def contains(e: E): Stream[QResult[E, V]] = Stream.empty

    override def notContains(e: E): Stream[QResult[E, V]] = Stream.empty

    override def gt(e: E): Stream[QResult[E, V]] = Stream.empty

    override def gte(e: E): Stream[QResult[E, V]] = Stream.empty

    override def iterator: Iterator[QResult[E, V]] = Iterator.empty
  }

  /**
   * @param c
   * @param interval
   * @param minL the start of left subtree span
   * @param maxL the end of left subtree span
   * @param minR the start of right subtree span
   * @param maxR the end of right subtree span
   * @param value
   * @param l
   * @param r
   * @param ordering$E$0
   * @tparam E
   * @tparam I
   * @tparam V
   */
  case class RBNode[E: Ordering, I <: Interval[E], V](
      c: Color,
      interval: I,
      minL: E,
      maxL: E,
      minR: E,
      maxR: E,
      value : V,
      l: RBTree[E, I, V],
      r: RBTree[E, I, V])
      extends RBTree[E, I, V] with QResult[E, V] {
    import o._

    def this(c : Color,
             interval: I,
             value : V,
             lRng : (E, E),
             l: RBTree[E, I, V],
             rRng : (E, E),
             r: RBTree[E, I, V])(o : Ordering[E]) = {
      this(
        c,
        interval,
        lRng._1, lRng._2,
        rRng._1, rRng._2,
        value,
        l,
        r
      )
    }

    /**
     * return all intervals that come before 'e'
     *
     * if e <= minL => nothing on left
     * if interval.start < e include this
     * if e <= minR nothing on right
     * @param e
     * @return
     */
    override def lt(e: E): Stream[QResult[E, V]] = {
      (if (e <= minL) Stream.empty else l.lt(e)) #:::
        (if (interval.start < e) Stream(this) else Stream.empty) #:::
        (if (e <= minR) Stream.empty else r.lt(e))
    }

    /**
     * return all intervals that contain or come before 'e'
     *
     * if e < minL => nothing on left
     * if interval.start <= e include it
     * if e < minR nothing on right
     * @param e
     * @return
     */
    override def lte(e: E): Stream[QResult[E, V]] = {
      (if (e < minL) Stream.empty else l.lte(e)) #:::
        (if (interval.start <= e) Stream(this) else Stream.empty) #:::
        (if (e < minR) Stream.empty else r.lte(e))
    }

    /**
     * return all intervals that contain 'e'
     *
     * if (e < minL || e >= maxL) nothing on left
     * if interval.contains(e) include it
     * if (e < minR || e >= maxR) nothing on right
     * @param e
     * @return
     */
    override def contains(e: E): Stream[QResult[E, V]] = {
      (if (e < minL || e >= maxL) Stream.empty else l.contains(e)) #:::
        (if (interval.contains(e)) Stream(this) else Stream.empty) #:::
        (if (e < minR || e >= maxR) Stream.empty else r.contains(e))
    }

    override def notContains(e: E): Stream[QResult[E, V]] = {
      l.notContains(e) #:::
        (if (!interval.contains(e)) Stream(this) else Stream.empty) #:::
        r.notContains(e)
    }

    /**
     * return all intervals that come after 'e'
     *
     * if  e > maxL nothing on left
     * if interval.start > e then include this
     * if e >= maxR nothing on right
     * @param e
     * @return
     */
    override def gt(e: E): Stream[QResult[E, V]] = {
      (if (e > maxL) Stream.empty else l.gt(e)) #:::
        (if (interval.start > e) Stream(this) else Stream.empty) #:::
        (if (e >= maxR) Stream.empty else r.gt(e))
    }

    /**
     * return all intervals that contain or come after 'e'
     *
     * if  e > maxL nothing on left
     * if interval.start >= e include it
     * if e >= maxR nothing on right
     * @param e
     * @return
     */
    override def gte(e: E): Stream[QResult[E, V]] = {
      (if (e > maxL) Stream.empty else l.gte(e)) #:::
        (if (interval.start >= e) Stream(this) else Stream.empty) #:::
        (if (e >= maxR) Stream.empty else r.gte(e))
    }

    override def iterator: Iterator[QResult[E, V]] = {
      val self = this
      // in-order traversal
      new Iterator[QResult[E, V]] {
        private var stack = List[RBNode[E, I, V]]()

        private def pushLeftEdge(start : RBTree[E, I, V]) : Unit = {
          var nd = start
          while(!nd.isInstanceOf[Empty[E, I, V]]) {
            val n = nd.asInstanceOf[RBNode[E, I, V]]
            stack = n :: stack
            nd = n.l
          }
        }

        pushLeftEdge(self)

        override def hasNext: Boolean = stack.nonEmpty

        override def next(): QResult[E, V] = {
          val nextNode = stack.head
          stack = stack.tail
          pushLeftEdge(nextNode.r)
          nextNode
        }
      }
    }
  }

  private def int_span[E: Ordering, I <: Interval[E], V](i : I, child : RBTree[E, I, V]) : (E, E) = {
    val o = implicitly[Ordering[E]]
    child match {
      case _ : Empty[E, I, V] => (i.start, i.start)
      case c : RBNode[E, I, V] =>
        (
          if (o.compare(c.interval.start, c.minL) < 0 ) c.interval.start else c.minL,
          if (o.compare(c.interval.end, c.maxR) > 0) c.interval.end else c.maxR
        )
    }
  }

  /**
   * Given a List with no duplicates of (Interval[E], Value) pairs create a RBTree in O(n) time.
   * From Okasaki exercise 3.9
   * - A RBTree of size n is a full binary tree of height nearestFullTreeSmallerThan(n)
   *   with all black nodes
   * - plus a bottom level with red nodes. The red nodes are evenly distributed among the
   *   nodes of the immediate level.
   *   - first add as left children; then fill in remaining right child slots
   *
   * @param l must not contain duplicates. This is not checked.
   * @tparam E
   * @tparam I
   * @tparam V
   * @return
   */
  def create[E: Ordering, I <: Interval[E], V](l : IndexedSeq[(I, V)]) : RBTree[E, I, V] = {

    val lnOf2 = scala.math.log(2) // natural log of 2

    def nearestFullTreeSmallerThan(n: Int): Int = {
      def _log2(x: Int): Int = if ( x <= 1) 0 else (scala.math.log(x) / lnOf2).toInt

      (1 << _log2(n + 1)) - 1
    }

    val empty = new Empty[E, I, V]
    val o = empty.o

    def subtree(color : Color,
                x : (I, V),
                l : RBTree[E, I, V],
                r : RBTree[E, I, V]) : RBNode[E, I, V] =
      new RBNode[E, I, V](color, x._1, x._2, int_span(x._1, l), l, int_span(x._1, r), r)(o)

    // scalastyle:off line.size.limit
    def build(l: IndexedSeq[(I, V)], rb: Int): RBTree[E, I, V] = (l, rb) match {
      case (IndexedSeq(), 0) => empty
      case (IndexedSeq(), _) => throw new IllegalStateException(s"Red budget of $rb allocated for an empty list")
      case (x +: IndexedSeq(), rb) =>
        if (rb == 0) {
           subtree(Black, x, empty, empty)
        } else if (rb == 1) {
          subtree(Red, x, empty, empty)
        } else {
          throw new IllegalStateException(s"Red budget of $rb allocated for a singleton list")
        }
      case (x +: y +: IndexedSeq(), rb) => // SubTree(B, SubTree(R, Empty, x, Empty), y, Empty)
        if (rb == 1) {
          subtree(Black, y, subtree(Red, x, empty, empty), empty)
        } else {
          throw new IllegalStateException(s"Red budget of $rb allocated for a list of size 2")
        }
      case (x +: y +: z +: IndexedSeq(), rb) =>
        if (rb == 0) {
          subtree(Black, y, subtree(Black, x, empty, empty), subtree(Black, z, empty, empty))
        } else if (rb == 2) {
          subtree(Black, y, subtree(Red, x, empty, empty), subtree(Red, z, empty, empty))
        } else {
          throw new IllegalStateException(s"Red budget of $rb allocated for a list of size 3")
        }
      case _ =>
        val halves = l.splitAt(l.size / 2)
        subtree(Black, halves._2.head, build(halves._1, (rb + 1) / 2), build(halves._2.tail, rb / 2))
    }
    // scalastyle:on



    implicit val oi = implicitly[Ordering[Interval[E]]]
    val sortedL = l.sortWith {
      case (p1, p2) => oi.lt(p1._1, p2._1)
    }
    val redBudget = sortedL.size - nearestFullTreeSmallerThan(sortedL.size)
    build(sortedL, redBudget)
  }
}
