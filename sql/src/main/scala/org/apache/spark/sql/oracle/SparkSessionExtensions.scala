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

import org.apache.spark.sql.{SparkSessionExtensions => ApacheSparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.oracle.parsing.OraParser
import org.apache.spark.sql.oracle.rules.OraLogicalRules

/**
 * Include this class in ''spark.sql.extensions'' for these extensions to take effect.
 *
 * The [[OraParser]] is also used to ensure that the [[OraLogicalRules]] are
 * applied as an `extraOptimizations`. This ensures that the
 * [[OraLogicalRules]] are applied in the *User Provided Optimizers*
 * **Batch** of the [[org.apache.spark.sql.execution.SparkOptimizer]],
 * which comes after all built-in rewrites. The rewrites in
 * [[OraSQLPushdownRule]] assume the input [[LogicalPlan]] has been
 * transformed by all Spark rewrite rules.

 */
class SparkSessionExtensions extends Function1[ApacheSparkSessionExtensions, Unit] {

  override def apply(spkExtensions: ApacheSparkSessionExtensions): Unit = {
    spkExtensions.injectParser((_, pI) => new OraParser(pI))
  }
}
