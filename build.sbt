
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"

ThisBuild / libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18"

lazy val root = (project in file("."))
  .settings(
    name := "HSBCTest"
  )
