ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "Project"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.3"
