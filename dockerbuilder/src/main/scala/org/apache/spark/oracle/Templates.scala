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

object Templates {

  val spark_props_filename = "spark.oracle.properties"

  abstract class Template {
    def replacementMap(cfg : Config) : Map[String, String] = Map.empty
  }

  trait Spark extends Template {
    val spark_app_name = "$$spark.app.name$$"
    val spark_mem = "$$spark.driver.memory$$"
    val spark_cores = "$$spark.driver.cores$$"

    val spark_home = "$$spark_home$$"
    val spark_submit_options = "$$spark_submit_options$$"
    val spark_master = "$$spark_master$$"
    val spark_version = "$$spark_version$$"
    val spark_download_url = "$$spark_download_url$$"
    val spark_tar_file = "$$spark_tar_file$$"

    def sparkVersion(cfg : Config) : String = cfg.sparkVer
    def sparkHome(cfg : Config) : String = s"/usr/spark-${sparkVersion(cfg)}"
    def sparkMaster(cfg : Config) : String = s"local[${cfg.spark_cores}]"
    def sparkOraConfDir(cfg : Config) : String = s"${sparkHome(cfg)}/sparkOraConf"
    def sparkSubmitOptions(cfg : Config) : String =
      s"--properties-file ${sparkOraConfDir(cfg)}/${spark_props_filename}"

    abstract override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map(
        spark_cores -> cfg.spark_cores.toString,
        spark_mem -> cfg.spark_mem,
        spark_app_name -> "Spark_Oracle_Demo",
        spark_home -> sparkHome(cfg),
        spark_submit_options-> sparkSubmitOptions(cfg),
        spark_master -> sparkMaster(cfg),
        spark_version -> sparkVersion(cfg),
        spark_download_url -> cfg.spark_download_url.toURL.toString,
        spark_tar_file -> Utils.fileNm(cfg.spark_download_url.toURL)
      )
    }
  }

  trait Oracle extends Template {
    val oracle_jdbc_url = "$$oracle.jdbc.url$$"
    val oracle_user_name = "$$oracle.username$$"
    val oracle_password = "$$oracle.password$$"
    val oracle_wallet_folder = "$$oracle.wallet.folder$$"
    val oracle_wallet_loc = "$$oracle.wallet.loc$$"

    def oracleJdbcUrl(cfg : Config) : String = cfg.oracle_instance_jdbc_url.toASCIIString
    def oracleUser(cfg : Config) : String = cfg.oracle_instance_username
    def oraclePassword(cfg : Config) : String =
      Option(cfg.oracle_instance_password).getOrElse("<unspecified>")
    def oracleWalletLoc(cfg : Config) : String =
      Option(cfg.oracle_instance_wallet_loc).
        map(_ => s"/usr/${cfg.oracle_wallet_folder_name}").
        getOrElse("<unspecified>")


    abstract override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map(
        oracle_jdbc_url -> oracleJdbcUrl(cfg),
        oracle_user_name -> oracleUser(cfg),
        oracle_password -> oraclePassword(cfg),
        oracle_wallet_folder -> cfg.oracle_wallet_folder_name,
        oracle_wallet_loc -> oracleWalletLoc(cfg)
      )
    }
  }

  trait SparkOra extends Template with Spark {
    val spark_ora_zip = "$$spark_ora_zip$$"
    val metadata_cache = "$$metadata_cache_loc$$"
    val sparkora_conf_dir = "$$spark_ora_conf_dir$$"
    val sparkora_log_dir = "$$spark_ora_log_dir$$"

    def metadataCacheLoc(cfg : Config) : String = s"${sparkHome(cfg)}/metadata_cache"

    def sparkOraLogDir(cfg : Config) : String = s"${sparkHome(cfg)}/sparkora_logs"

    abstract override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map(
        spark_ora_zip -> cfg.spark_ora_zip.getName,
        metadata_cache -> metadataCacheLoc(cfg),
        sparkora_conf_dir -> sparkOraConfDir(cfg),
        sparkora_log_dir -> sparkOraLogDir(cfg)
      )
    }
  }

  trait Zeppelin extends Template {
    val zeppelin_version = "$$zeppelin_version$$"
    val zeppelin_home = "$$zeppelin_home$$"
    val zeppelin_tar_file = "$$zeppelin_tar_file$$"
    val zeppelin_download_url = "$$zeppelin_download_url$$"

    def zepVersion(cfg : Config) : String = cfg.zepVer
    def zepHome(cfg : Config) : String = s"/usr/zeppelin-${zepVersion(cfg)}"


    abstract override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map(
        zeppelin_version -> zepVersion(cfg),
        zeppelin_home -> zepHome(cfg),
        zeppelin_download_url -> cfg.zeppelin_download_url.toURL.toString,
        zeppelin_tar_file -> Utils.fileNm(cfg.zeppelin_download_url.toURL)
      )
    }
  }

  object Interpreter_Json extends Template with Spark with Zeppelin {

    val template = "zeppelin.interpreter.json.template"
    val outFileName = "interpreter.json"

    override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map.empty
    }
  }

  object Spark_Properties extends Template with Spark with Oracle with SparkOra {
    val template = "spark.conf.template"
    val outFileName = spark_props_filename

    override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map.empty
    }
  }

  object Zeppelin_Env extends Template with Spark with SparkOra with Zeppelin {
    val template = "zeppelin-env.template"
    val outFileName = "zeppelin-env.sh"

    override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map.empty
    }
  }

  object Spark_Ora_Log extends Template with Spark with SparkOra {
    val template = "log4j.properties.template"
    val outFileName = "log4j-driver.properties"

    override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map.empty
    }
  }

  object Dockerfile extends Template with Spark with Oracle with SparkOra with Zeppelin {
    val template = "Dockerfile.template"
    val outFileName = "Dockerfile"

    override def replacementMap(cfg : Config) : Map[String, String] = {
      super.replacementMap(cfg) ++ Map.empty
    }
  }

}
