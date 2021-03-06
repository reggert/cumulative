name := "cumulative"

organization in ThisBuild := "io.github.reggert.cumulative"

version in ThisBuild := "0.0.1-SNAPSHOT"

lazy val scala2_10 = "2.10.7"
lazy val scala2_11 = "2.11.12"
lazy val scala2_12 = "2.12.8"
lazy val scalaVersions = Seq(scala2_10, scala2_11, scala2_12)

scalaVersion in ThisBuild := scala2_12

javacOptions in ThisBuild ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions in ThisBuild ++= Seq("-target:jvm-1.8", "-deprecation", "-feature")

lazy val accumuloVersion = "1.9.3"

lazy val accumuloCore = (
  "org.apache.accumulo" % "accumulo-core" % accumuloVersion % Compile
    exclude("javax.activation", "activation")
    exclude("log4j", "log4j")
    exclude("javax.servlet", "javax.servlet-api")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("commons-logging", "commons-logging")
)

lazy val miniAccumuloCluster = (
  "org.apache.accumulo" % "accumulo-minicluster" % accumuloVersion % Test
    exclude("javax.activation", "activation")
    exclude("log4j", "log4j")
    exclude("javax.servlet", "javax.servlet-api")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("commons-logging", "commons-logging")
)

lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4" % Test

lazy val scalaARM = "com.jsuereth" %% "scala-arm" % "2.0" % Compile

val log4jVersion = "2.10.0"
lazy val log4jAPI = "org.apache.logging.log4j" % "log4j-api" % log4jVersion % Compile
lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % log4jVersion % Runtime
lazy val log4jSLF4J = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % Test

lazy val slf4jAPI = "org.slf4j" % "slf4j-api" % "1.7.25" % Compile

lazy val jclOverSLF4J = "org.slf4j" % "jcl-over-slf4j" % "1.7.25" % Test

lazy val scalaMock = "org.scalamock" %% "scalamock" % "4.0.0" % Test

val hadoopGroupId = "org.apache.hadoop"
val hadoopVersion = "3.2.0"
lazy val hadoopClientAPI = hadoopGroupId % "hadoop-client-api" % hadoopVersion % Compile
lazy val hadoopClientRuntime = hadoopGroupId % "hadoop-client-runtime" % hadoopVersion % Compile
lazy val hadoopMapReduce = hadoopGroupId % "hadoop-mapreduce" % hadoopVersion % Compile

/*
  This is needed by the processes started by MiniAccumuloCluster, which apparently
  touch the internals of Log4J 1.2 and not just the API. I'm looking at you, ZooKeeper. >:-(
 */
lazy val log4j1 = "log4j" % "log4j" % "1.2.17" % Test

lazy val scalaSwing = "org.scala-lang.modules" %% "scala-swing" % "2.1.1"

lazy val scopt = "com.github.scopt" %% "scopt" % "4.0.0-RC2"

lazy val core = (project in file("cumulative-core")).settings(
  name := "cumulative-core",
  autoScalaLibrary := true,
  libraryDependencies := Seq(
    accumuloCore,
    miniAccumuloCluster,
    hadoopClientAPI,
    hadoopClientRuntime,
    log4jAPI,
    log4jCore,
    log4jSLF4J,
    slf4jAPI,
    jclOverSLF4J,
    log4j1,
    scalaTest,
    scalaMock,
    scalaARM,
    "org.scala-lang" % "scala-library" % scalaVersion.value % Compile
  ),
  crossScalaVersions := scalaVersions,
  fork in Test := true
)

lazy val model = (project in file("cumulative-model")).settings(
  name := "cumulative-model",
  autoScalaLibrary := true,
  libraryDependencies := Seq(
    accumuloCore,
    scalaTest,
    scalaMock,
    "org.scala-lang" % "scala-library" % scalaVersion.value % Compile
  ),
  crossScalaVersions := scalaVersions,
  fork in Test := true
).dependsOn(core)

lazy val ui = (project in file("cumulative-ui")).settings(
  name := "cumulative-ui",
  autoScalaLibrary := true,
  libraryDependencies := Seq(
    accumuloCore,
    scalaTest,
    scalaMock,
    scalaSwing,
    scopt,
    "org.scala-lang" % "scala-library" % scalaVersion.value % Compile
  ),
  crossScalaVersions := scalaVersions,
  fork in Test := true
).dependsOn(core, model)

