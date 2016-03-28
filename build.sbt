import sbt._
import Process._
import Keys._

lazy val root = (project in file(".")).
  settings(
    name := "ohdsi-project",
    version := "1.0",
    scalaVersion := "2.11.7"
  )

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"  % "1.6.1",
  "org.apache.spark"  %% "spark-sql" % "1.6.1",
  "org.apache.spark"  %% "spark-mllib"  % "1.6.1",
  "org.apache.spark"  %% "spark-graphx" % "1.6.1",
  "com.tumblr" %% "colossus" % "0.7.0"
)
