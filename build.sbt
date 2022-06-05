import sbt.Keys._
import sbt._

name := "ScalaChallenge"

version := "0.1"

val sparkVersion = "3.1.1"
val scalaVersion = "2.13"

//Spark dependencies
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val readFileDependencies = Seq(
  "com.lihaoyi" %% "os-lib" % "0.8.0"
)++sparkDependencies

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.12" % Test
)++readFileDependencies

libraryDependencies ++= testDependencies