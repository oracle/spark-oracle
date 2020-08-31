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

import java.io._
import java.net.{URI, URL}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor, StandardCopyOption}

import scala.io.Source

object Utils {

  def generateFromTemplate(templateResource: String,
                           replacements: Map[String, String],
                           outFile : File): Unit = {
    val stream = getClass.getClassLoader.getResourceAsStream(templateResource)


    val template = Source.fromInputStream(stream).mkString
    val text = replacements.foldLeft(template)(
      (a, kv) => a.replace(kv._1, kv._2)
    )

    val bw = new BufferedWriter(new FileWriter(outFile))
    try {
      bw.write(text)
    } finally {
      bw.close()
    }
  }

  def downloadURL(url : URL,
                  outFile : File,
                  log : String => Unit) : Unit = {
    // import sys.process._
    // url #> outFile !!

    val inputStream = new BufferedInputStream(url.openStream)
    val fileOS = new FileOutputStream(outFile)
    val bufSz : Int = 1024 * 1024 * 32

    try {
      val data = new Array[Byte](bufSz)
      var iter = 0
      var byteContent = 0
      while ({byteContent = inputStream.read(data, 0, bufSz); byteContent} != -1) {
        fileOS.write(data, 0, byteContent)
        iter += 1
        if (iter % 400 == 0) {
          log(".")
        }
      }
    } finally {
      if (inputStream != null) inputStream.close()
      if (fileOS != null) fileOS.close()
    }
  }

  /**
   * Download URL to current directory
   * @param url
   */
  def downloadURL(uri : URI,
                  log : String => Unit) : Unit = {
    val url = uri.toURL
    val nm : String = fileNm(url)
    val outFile = new File(nm)
    downloadURL(url, outFile, log)
  }

  def validateURL(url : URL) : Either[String, Unit] = {
    val conn = url.openConnection()

    try {
      conn.connect()
      Right(())
    } catch {
      case e : Exception => Left(
        s"""Failed to validate download url ${url.toString}:
           |  ${e.getMessage}""".stripMargin
      )
    }
  }

  // for example spark-3.1.1-bin-hadoop3.2.tgz
  val SPARK_TARFILE_REGEX = "spark-([0-9])\\.([0-9])\\.([0-9])-(.*?)\\.tgz".r

  // for example zeppelin-0.9.0-bin-netinst.tgz
  val ZEPPELIN_TARFILE_REGEX = "zeppelin-([0-9])\\.([0-9])\\.([0-9])-(.*?)\\.tgz".r

  def fileNm(downloadURL : URL) : String = downloadURL.getPath.split('/').last

  def extractSparkVersion(uri : URI) : (Int, Int, Int) = {
    val downloadURL = uri.toURL

    val nm = fileNm(downloadURL)
    nm match {
      case SPARK_TARFILE_REGEX(v, mj, mnr, rest) => (v.toInt, mj.toInt, mnr.toInt)
      case _ =>
        throw new IllegalArgumentException(
          s"""file ${nm} in URI(${uri}) doesn't look like a spark download
             |  failed to match pattern: ${SPARK_TARFILE_REGEX.regex}""".stripMargin
        )
    }
  }

  def isValidSparkVer(ver : Int, major : Int, minor : Int) :
  Option[String] = (ver, major, major) match {
    case (3, 1, mn) if mn >= 1 => None
    case (3, mjr, _) if mjr > 1 => None
    case _ => Some(s"Currently not supported Spark version '${ver}.${major}.${minor}'")
  }

  def extractZeppelinVersion(uri : URI) : (Int, Int, Int) = {
    val downloadURL = uri.toURL

    val nm = fileNm(downloadURL)
    nm match {
      case ZEPPELIN_TARFILE_REGEX(v, mj, mnr, rest) => (v.toInt, mj.toInt, mnr.toInt)
      case _ =>
        throw new IllegalArgumentException(
          s"""file ${nm} in URI(${uri}) doesn't look like a spark download
             |  failed to match pattern: ${ZEPPELIN_TARFILE_REGEX.regex}""".stripMargin
        )
    }
  }

  def isValidZeppelinVer(ver : Int, major : Int, minor : Int) :
  Option[String] = (ver, major, major) match {
    case (0, 9, mn) if mn >= 0 => None
    case _ => Some(s"Currently not supported Zeppelin version '${ver}.${major}.${minor}'")
  }

  @throws[IOException]
  def isZipFile(file: File): Boolean = {
    if (file.isDirectory) {
      false
    } else if (!file.canRead) {
      false
    } else if (file.length < 4) {
      false
    } else {
      val in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
      try {
        in.readInt == 0x504b0304
      } catch {
        case oie: IOException => false
      } finally {
        in.close()
      }
    }
  }

  def copyFile(file : File) : Unit = {
    val srcPath = file.toPath
    val destPath = Paths.get("").resolve(file.getName)
    Files.copy(srcPath, destPath,
      StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING
    )
  }

  def copyFolder(file : File,
                 destFolderNm : String) : Unit = {
    val srcPath = file.toPath
    val destPath = Paths.get("").resolve(destFolderNm)

    // copied from
    // https://stackoverflow.com/questions/6214703/copy-entire-directory-contents-to-another-directory
    Files.walkFileTree(srcPath, new SimpleFileVisitor[Path]() {
      @throws[IOException]
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.createDirectories(destPath.resolve(srcPath.relativize(dir)))
        FileVisitResult.CONTINUE
      }

      @throws[IOException]
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.copy(file, destPath.resolve(srcPath.relativize(file)))
        FileVisitResult.CONTINUE
      }
    })
  }

  def makeFolder(destFolderNm : String) : Unit = {
    val destPath = Paths.get("").resolve(destFolderNm)
    if (!Files.exists(destPath)) {
      Files.createDirectory(destPath)
    }
  }
}
