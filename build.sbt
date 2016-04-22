import sbt._
import Process._
import Keys._

lazy val root = (project in file(".")).
  settings(
    name := "ohdsi-project",
    version := "1.0",
    scalaVersion := "2.11.8"
  ).enablePlugins(DockerPlugin)

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"  % "1.6.1",
  "org.apache.spark"  %% "spark-sql" % "1.6.1",
  "org.apache.spark"  %% "spark-mllib"  % "1.6.1",
  "org.apache.spark"  %% "spark-graphx" % "1.6.1",
  // config items
  "com.typesafe" % "config" % "1.3.0",
  // test
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.mockito" % "mockito-all" % "2.0.2-beta" % "test",
  // json
  "io.spray" %% "spray-json" % "1.3.2",
  // csv
  "com.databricks" %% "spark-csv" % "1.4.0",
  // time
  "com.github.nscala-time" %% "nscala-time" % "2.12.0",
  // save files
  "com.github.pathikrit" %% "better-files" % "2.15.0"
)

// command c
fork in run := false
// no parallel tests
parallelExecution in Test := false

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
