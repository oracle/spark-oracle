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
package org.apache.spark.oracle

import java.io.File
import java.net.URI

case class Config(
    spark_mem : String = "1g",
    spark_cores : Int = -1,
    oracle_instance_jdbc_url : URI = null,
    oracle_instance_username : String = null,
    oracle_instance_password : String = null,
    oracle_instance_wallet_loc : File = null,
    spark_download_url : URI = null,
    zeppelin_download_url : URI = null,
    spark_ora_zip : File = null) {

  val oracle_wallet_folder_name = "oracle_wallet"

  def isValidateOraclePasswordSpec : Option[String] = {
    if (oracle_instance_password == null && oracle_instance_wallet_loc == null) {
      Some(s"Must provide oracle instance password or a wallet location")
    } else None
  }

  def sparkVer : String = {
    val (v, mjr, mnr) = Utils.extractSparkVersion(spark_download_url)
    s"${v}.${mjr}.${mnr}"
  }

  def zepVer : String = {
    val (v, mjr, mnr) = Utils.extractZeppelinVersion(zeppelin_download_url)
    s"${v}.${mjr}.${mnr}"
  }

  override def toString: String = {

    def passWalletStr : String = (oracle_instance_password, oracle_instance_wallet_loc) match {
      case (p, null) => s"password: ${p}"
      case (null, w) => s"wallet_loc: ${w}"
      case (p, w) => s"password: ${p}\n    wallet_loc: ${w}"
      case _ => ""
    }

    s"""
       |Spark Oracle Docker Configuration specified:
       |  spark_mem=${spark_mem}, spark_cores=${spark_cores}
       |  Oracle instance:
       |    jdbc_url: ${oracle_instance_jdbc_url}
       |    user_name: ${oracle_instance_username}
       |    ${passWalletStr}
       |  Spark:
       |    download url: ${spark_download_url}
       |    version: ${sparkVer}
       |  Zeppelin:
       |   download url: ${zeppelin_download_url}
       |   version: ${zepVer}
       |  Spark_Oracle:
       |    zip location: ${spark_ora_zip}
       |""".stripMargin
  }
}

// scalastyle:off line.size.limit
object Config {
  import scopt.OParser
  val builder = OParser.builder[Config]

  private val MEM_SPEC = "([0-9]+)([mMgG])".r

  val parser = {
    import builder._
    OParser.sequence(
      programName("sparkOraDockerBuilder"),
      head("sparkOraDockerBuilder", "0.1.1"),

      opt[String]('m', "spark_mem")
        .required()
        .action((x, c) => c.copy(spark_mem = x))
        .validate {
          case MEM_SPEC(_, _) => Right(())
          case x => Left(s"spark_mem value '${x}' doesn't match pattern ${MEM_SPEC.regex}")
        }
        .text(
          """memory in Mb/Gb for spark; for example 512m or 2g.
            |when running the tpcds demo set it to 4g""".stripMargin),

      opt[Int]('c', "spark_cores")
        .required()
        .action((x, c) => c.copy(spark_cores = x))
        .text(
          """num_cores for spark.
            |when running the tpcds demo set it to at-least 4""".stripMargin),

      opt[URI]('j', "oracle_instance_jdbc_url")
        .required()
        .action((x, c) => c.copy(oracle_instance_jdbc_url = x))
        .validate {
          case uri if uri.getScheme == "jdbc" => Right(())
          case x => Left(s"oracle_instance_jdbc_url value '${x}' doesn't appear to be correct; scheme is ${x.getScheme}")
        }
        .text(
          """jdbc connection information for the oracle instance.
            |for example: "jdbc:oracle:thin:@10.89.206.230:1531/cdb1_pdb7.regress.rdbms.dev.us.oracle.com
            |
            |specify the ip-addr of host; otherwise you may need
            |to edit the /etc/resolv.conf of the docker container."""".stripMargin)
        ,

      opt[String]('u', "oracle_instance_username")
        .required()
        .action((x, c) => c.copy(oracle_instance_username = x))
        .text(
          """Oracle username to connect the oracle instance""".stripMargin)
      ,

      opt[String]('p', "oracle_instance_password")
        .optional()
        .action((x, c) => c.copy(oracle_instance_password = x))
        .text(
          """Oracle password for the oracle user.
            |Either provide the password or location of a wallet.""".stripMargin)
      ,

      opt[File]('w', "oracle_instance_wallet_loc")
        .optional()
        .action((x, c) => c.copy(oracle_instance_wallet_loc = x))
        .validate {x =>
          if (!x.exists() || !x.isDirectory) {
            Left(s"Wallet location ${x} must exist and must be a folder.")
          } else Right(())
        }
        .text(
          """Oracle password for the oracle user.
            |Either provide the password or location of a wallet.""".stripMargin)
      ,

      opt[URI]('s', "spark_download_url")
        .required()
        .action((x, c) => c.copy(spark_download_url = x))
        .validate { x =>
          val t = scala.util.Try(Utils.extractSparkVersion(x)).toEither
          if (t.isLeft) {
            failure(t.left.get.getMessage)
          } else {
            val (v, mjr, mnr) = t.right.get
            Utils.isValidSparkVer(v, mjr, mnr).map(failure).getOrElse(success)
          }
        }
        .validate(x => Utils.validateURL(x.toURL))
        .text(
          """url to download apache spark. Spark version must be 3.1.0 or above.
            |for example: https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz""".stripMargin)
      ,

      opt[URI]('z', "zeppelin_download_url")
        .required()
        .action((x, c) => c.copy(zeppelin_download_url = x))
        .validate { x =>
          val t = scala.util.Try(Utils.extractZeppelinVersion(x)).toEither
          if (t.isLeft) {
            failure(t.left.get.getMessage)
          } else {
            val (v, mjr, mnr) = t.right.get
            Utils.isValidZeppelinVer(v, mjr, mnr).map(failure).getOrElse(success)
          }
        }
        .validate(x => Utils.validateURL(x.toURL))
        .text(
          """url to download apache zeppelin. Spark version must be 0.9.0 or above.
            |for example: https://downloads.apache.org/zeppelin/zeppelin-0.9.0/zeppelin-0.9.0-bin-netinst.tgz""".stripMargin)
      ,

      opt[File]('o', "spark_ora_zip")
        .required()
        .action((x, c) => c.copy(spark_ora_zip = x))
        .validate {x =>
          if (!x.exists() || !x.isFile) {
            Left(s"Spark Oracle package ${x} must exist.")
          } else {
            if (Utils.isZipFile(x)) {
              Right(())
            } else {
              Left(s"Spark Oracle package ${x} must be a zip file.")
            }
          }
        }
        .text(
          """location of spark-oracle package.
            |for example: ~/Downloads/spark-oracle-0.1.0-SNAPSHOT.zip""".stripMargin)
      ,

      checkConfig( c =>
      c.isValidateOraclePasswordSpec.map(failure).getOrElse(success)
      )
    )
  }

}
