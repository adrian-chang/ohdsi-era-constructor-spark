import sbt._
import Process._
import Keys._

lazy val root = (project in file(".")).
  settings(
    name := "ohdsi-project",
    version := "1.0",
    scalaVersion := "2.11.7"
  ).enablePlugins(DockerPlugin)

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"  % "1.6.1",
  "org.apache.spark"  %% "spark-sql" % "1.6.1",
  "org.apache.spark"  %% "spark-mllib"  % "1.6.1",
  "org.apache.spark"  %% "spark-graphx" % "1.6.1",
  // web server
  "com.tumblr" %% "colossus" % "0.7.0",
  // config items
  "com.typesafe" % "config" % "1.3.0",
  // test
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  // postgres
  "org.postgresql" % "postgresql" % "9.4.1208",
  "io.spray" %%  "spray-json" % "1.3.2"
)

// command c
fork in run := true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

dockerfile in docker := {
  // The assembly task generates a fat JAR file
  val artifact: File = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"

  new Dockerfile {
    from("java")
    add(artifact, artifactTargetPath)
    entryPoint("java", "-jar", artifactTargetPath)
  }
}

buildOptions in docker := BuildOptions(cache = false)

// hook spark back in during sbt run
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
