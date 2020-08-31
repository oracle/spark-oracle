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

import scopt.OParser

object DockerBuilder {

  def log(s : Any) : Unit = {
    // scalastyle:off println
    print(s.toString)
    // scalastyle:on println
  }

  def run(cfg : Config) : Unit = {
    import cfg._

    log("\nSetting up configuration\n")

    log("Copy spark oracle zip\n")
    Utils.copyFile(cfg.spark_ora_zip)

    log("Copy or create empty wallet folder\n")
    if (cfg.oracle_instance_wallet_loc != null) {
      Utils.copyFolder(oracle_instance_wallet_loc, cfg.oracle_wallet_folder_name)
    } else {
      Utils.makeFolder(cfg.oracle_wallet_folder_name)
    }

    log("Setup zeppelin interpreter.json\n")
    Utils.generateFromTemplate(Templates.Interpreter_Json.template,
      Templates.Interpreter_Json.replacementMap(cfg),
      new File(Templates.Interpreter_Json.outFileName)
    )

    log("Setup spark properties file\n")
    Utils.generateFromTemplate(Templates.Spark_Properties.template,
      Templates.Spark_Properties.replacementMap(cfg),
      new File(Templates.Spark_Properties.outFileName)
    )

    log("Setup zeppelin-env.sh file\n")
    Utils.generateFromTemplate(Templates.Zeppelin_Env.template,
      Templates.Zeppelin_Env.replacementMap(cfg),
      new File(Templates.Zeppelin_Env.outFileName)
    )

    log("Setup log4j-driver.properties file\n")
    Utils.generateFromTemplate(Templates.Spark_Ora_Log.template,
      Templates.Spark_Ora_Log.replacementMap(cfg),
      new File(Templates.Spark_Ora_Log.outFileName)
    )

    log("Setup Dockerfile\n")
    Utils.generateFromTemplate(Templates.Dockerfile.template,
      Templates.Dockerfile.replacementMap(cfg),
      new File(Templates.Dockerfile.outFileName)
    )

  }

  def main(args : Array[String]) : Unit = {
    OParser.parse(Config.parser, args, Config()) match {
      case Some(config) =>
        log(config)
        run(config)
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

}
