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

package org.apache.spark.sql

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.trees.TreeNode

package object oracle {

  private[oracle] trait UnSupportedActionHelper[T <: TreeNode[T]] {

    /*
     * Such as 'Illegal' or 'Unsupported'
     */
    def unsupportVerb: String

    /*
     * Such as 'OraPlan build action'
     */
    def actionKind: String

    def apply(action: String, node: T, reason: Option[String] = None): Nothing = {
      val reasonStr = if (reason.isDefined) {
        s"\n  reason: ${reason.get}\n"
      } else ""

      val nodeStr = if (node != null) {
        s" on\n  node: ${node}"
      } else ""

      throw new UnsupportedOperationException(
        s"${unsupportVerb} ${actionKind}: ${action}${nodeStr}${reasonStr}")
    }

    def apply(action: String, node: T, reason: String): Nothing =
      apply(action, node, Some(reason))

  }

  implicit def toOraSparkSess(sparkSession: SparkSession) : OraSparkSessionExts =
    OraSparkSessionExts(sparkSession)

  implicit def toSparkSess(oSS : OraSparkSessionExts) : SparkSession = oSS.sparkSession
}
