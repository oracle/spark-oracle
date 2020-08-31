/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.spark.datastructs

import java.util.Locale

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.oracle.{OraSparkConfig, OraSparkUtils}

/**
 *
 * A replacement for [[org.apache.spark.sql.catalyst.util.CaseInsensitiveMap]] where
 * key resolution follows the following logic:
 * - when `spark.sql.caseSensitive=false`(the default)
 *   - behave like `CaseInsensitiveMap`
 * - when `spark.sql.caseSensitive=true`
 *   - try to resolve in a case-sensitive way
 *   - if above fails resolve in a case-insensitive way.
 *
 */
class SQLIdentifierMap[T] private (val originalMap: Map[String, T]) extends Map[String, T]
  with Serializable {

  val keyLowerCasedMap = originalMap.map(kv => kv.copy(_1 = kv._1.toLowerCase(Locale.ROOT)))

  private def checkCaseSensitive : Boolean = {
    OraSparkConfig.getConf(SQLConf.CASE_SENSITIVE)
  }

  override def get(k: String): Option[T] = {
    if (checkCaseSensitive) {
      originalMap.get(k).orElse(keyLowerCasedMap.get(k.toLowerCase(Locale.ROOT)))
    } else {
      keyLowerCasedMap.get(k.toLowerCase(Locale.ROOT))
    }
  }

  override def +[V1 >: T](kv: (String, V1)): SQLIdentifierMap[V1] = {
    if (checkCaseSensitive) {
      new SQLIdentifierMap(originalMap + kv)
    } else {
      new SQLIdentifierMap(originalMap.filter(!_._1.equalsIgnoreCase(kv._1)) + kv)
    }
  }

  def ++(xs: TraversableOnce[(String, T)]): SQLIdentifierMap[T] = {
    xs.foldLeft(this)(_ + _)
  }

  override def iterator: Iterator[(String, T)] = {
    if (checkCaseSensitive) {
      originalMap.iterator
    } else {
      keyLowerCasedMap.iterator
    }
  }

  override def -(key: String): Map[String, T] = {
    if (checkCaseSensitive) {
      new SQLIdentifierMap(originalMap.filter(!_._1.equals(key)))
    } else {
      new SQLIdentifierMap(originalMap.filter(!_._1.equalsIgnoreCase(key)))
    }
  }
}

object SQLIdentifierMap {
  def apply[T](params: Map[String, T]): SQLIdentifierMap[T] = params match {
    case caseSensitiveMap: SQLIdentifierMap[T] => caseSensitiveMap
    case _ => new SQLIdentifierMap(params)
  }
}
