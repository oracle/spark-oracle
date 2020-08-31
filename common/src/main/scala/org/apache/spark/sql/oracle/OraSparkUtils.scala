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

import java.util.Locale

import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.oracle.util.CrossProductIterator
import org.apache.spark.sql.types.{DataType, FractionalType, NumericType, StructType}
import org.apache.spark.util.Utils

object OraSparkUtils {

  def getLocalDir(conf: SparkConf): String = {
    Utils.getLocalDir(conf)
  }

  def isNumeric(dt: DataType): Boolean = NumericType.acceptsType(dt)
  def isApproximateNumeric(dt: DataType): Boolean = dt.isInstanceOf[FractionalType]

  def setLogLevel(logLevel: String): Unit = {
    val upperCased = logLevel.toUpperCase(Locale.ENGLISH)
    org.apache.spark.util.Utils.setLogLevel(org.apache.log4j.Level.toLevel(logLevel))
  }

  def currentSparkSessionOption : Option[SparkSession] = {
    var spkSessionO = SparkSession.getActiveSession
    if (!spkSessionO.isDefined) {
      spkSessionO = SparkSession.getDefaultSession
    }
    spkSessionO
  }

  def currentSparkSession: SparkSession = {
    currentSparkSessionOption.getOrElse(???)
  }

  def getSparkClassLoader: ClassLoader = Utils.getSparkClassLoader

  def currentSparkContext: SparkContext = currentSparkSession.sparkContext

  def currentSQLConf: SQLConf = {
    var spkSessionO = SparkSession.getActiveSession
    if (!spkSessionO.isDefined) {
      spkSessionO = SparkSession.getDefaultSession
    }

    spkSessionO.map(_.sqlContext.conf).getOrElse {
      val sprkConf = SparkEnv.get.conf
      val sqlConf = new SQLConf
      sprkConf.getAll.foreach {
        case (k, v) =>
          sqlConf.setConfString(k, v)
      }
      sqlConf
    }
  }

  def dataFrame(lP: LogicalPlan)(implicit sqlContext: SQLContext): DataFrame = {
    Dataset.ofRows(sqlContext.sparkSession, lP)
  }

  def defaultParallelism(sparkSession: SparkSession) : Int =
    sparkSession.sparkContext.schedulerBackend.defaultParallelism()

  def throwAnalysisException[T](msg: => String): T = {
    throw new AnalysisException(msg)
  }

  private val r = new Random()

  def nextRandomInt(n: Int): Int = r.nextInt(n)

  /**
   * from fpinscala book
   *
   * @param a
   * @tparam A
   * @return
   */
  def sequence[A](a: Seq[Option[A]]): Option[Seq[A]] =
    a match {
      case Nil => Some(Nil)
      case s => s.head flatMap (hh => sequence(s.tail) map (hh +: _))
    }

  def crossProduct[T](seqs: Seq[Seq[T]]): Iterator[Seq[T]] = {
    val rseqs = seqs.reverse
    rseqs.tail.foldLeft(CrossProductIterator[T](None, rseqs.head)) {
      case (currItr, nSeq) => CrossProductIterator[T](Some(currItr), nSeq)
    }
  }

}
