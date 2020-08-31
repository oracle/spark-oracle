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

import Dependencies._
import sbt.Keys.test
import sbt._

ThisBuild / scalaVersion := Versions.scalaVersion
ThisBuild / crossScalaVersions := Seq(Versions.scalaVersion)

ThisBuild / homepage := Some(url("https://github.com/oracle/spark-oracle"))
ThisBuild / licenses := List("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / organization := "com.oracle.spark"
ThisBuild / organizationName := "Oracle"
ThisBuild / version := Versions.sparkOracleVersion

// from https://www.scala-sbt.org/1.x/docs/Cached-Resolution.html
// added to commonSettings
// ThisBuild / updateOptions := updateOptions.value.withLatestSnapshots(false)
// ThisBuild / updateOptions := updateOptions.value.withCachedResolution(true)

Global / resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.sonatypeRepo("public"),
  "Apache snapshots repo" at "https://repository.apache.org/content/groups/snapshots/")

lazy val commonSettings = Seq(
  updateOptions := updateOptions.value.withLatestSnapshots(false),
  updateOptions := updateOptions.value.withCachedResolution(true),
  javaOptions := Seq(
    "-Xms1g",
    "-Xmx3g",
    "-Duser.timezone=UTC",
    "-Dscalac.patmat.analysisBudget=512",
    "-XX:MaxPermSize=256M",
    "-Dspark.oracle.test.db_instance=scale1_tpcds",
    "-Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=n"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-feature", "-deprecation"),
  test in assembly := {},
  fork in Test := true,
  parallelExecution in Test := false,
  libraryDependencies ++= (scala.dependencies ++
    spark.dependencies ++
    utils.dependencies ++
    test_infra.dependencies),
  excludeDependencies ++= Seq(ExclusionRule("org.apache.calcite.avatica"))
)

lazy val common = project
  .in(file("common"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)

lazy val orastuff = project
  .in(file("orastuff"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= oracle.dependencies)
  .dependsOn(common)

lazy val macros = project
  .in(file("macros"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= scala.dependencies)
  .dependsOn(common)

lazy val sql = project
  .in(file("sql"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)
  .dependsOn(common, orastuff, macros % "compile->compile;test->test")

lazy val mllib = project
  .in(file("mllib"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)
  .dependsOn(common, orastuff)

lazy val spark_extend = project
  .in(file("packaging/spark_extend"))
  .enablePlugins(UniversalPlugin)
  .settings(commonSettings: _*)
  .settings(Assembly.assemblySettings: _*)
  // remove root folder; set jar name; add maintainer
  .settings(
    maintainer in Universal := "harish.butani@oracle.com",
    packageName in Universal := "spark-oracle-" + (version.value),
    topLevelDirectory in Universal := None,
    mappings in Universal += {
      val assemblyJar = (assembly).value
      assemblyJar -> ("jars/" + assemblyJar.getName)
    },
    mappings in Universal ++= Assembly
      .oraJarsToAdd((Runtime / externalDependencyClasspath).value)
      .map(f => f -> ("jars/" + f.getName)))
  .dependsOn(common, orastuff, sql, mllib)

lazy val spark_embed = project
  .in(file("packaging/spark_embed"))
  .settings(commonSettings: _*)
  .settings(Assembly.assemblySettings: _*)
  .dependsOn(common, orastuff, sql, mllib)

lazy val dockerbuilder = project.
  in(file("dockerbuilder"))
  .settings(commonSettings: _*)
  .settings(Assembly.dockerBuilderAssembly: _*)
  .settings(
    libraryDependencies := docker_builder.dependencies,
    publishArtifact in (Compile, packageBin) := false,
    publishArtifact in Test := false,
    aggregate in assembly := false
  )

