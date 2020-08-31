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

import java.util.Properties

import org.apache.spark.SparkException

object BuildInfo {

  lazy val (
    spark_ora_version: String,
    spark_ora_branch: String,
    spark_ora_revision: String,
    spark_ora_build_user: String,
    spark_ora_repo_url: String,
    spark_ora_build_date: String) = {

    val resourceStream = Thread
      .currentThread()
      .getContextClassLoader
      .getResourceAsStream("spark-oracle-version-info.properties")

    try {
      val unknownProp = "<unknown>"
      val props = new Properties()
      props.load(resourceStream)
      (
        props.getProperty("version", unknownProp),
        props.getProperty("branch", unknownProp),
        props.getProperty("revision", unknownProp),
        props.getProperty("user", unknownProp),
        props.getProperty("url", unknownProp),
        props.getProperty("date", unknownProp))
    } catch {
      case npe: NullPointerException =>
        throw new SparkException(
          "Error while locating file spark-oracle-version-info.properties",
          npe)
      case e: Exception =>
        throw new SparkException(
          "Error loading properties from spark-oracle-version-info.properties",
          e)
    } finally {
      if (resourceStream != null) {
        try {
          resourceStream.close()
        } catch {
          case e: Exception =>
            throw new SparkException("Error closing spark-oracle build info resource stream", e)
        }
      }
    }
  }

}
