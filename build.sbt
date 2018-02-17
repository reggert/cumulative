name := "cumulative"

organization in ThisBuild := "io.github.reggert.cumulative"

version in ThisBuild := "0.0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.4"

javacOptions in ThisBuild ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions in ThisBuild ++= Seq("-target:jvm-1.8", "-deprecation", "-feature")

lazy val accumuloVersion = "1.8.1"

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

lazy val log4jAPI = "org.apache.logging.log4j" % "log4j-api" % "2.10.0" % Compile

lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % "2.10.0" % Runtime

lazy val log4jSLF4J = "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.10.0" % Test

lazy val slf4jAPI = "org.slf4j" % "slf4j-api" % "1.7.25" % Compile

lazy val jclOverSLF4J = "org.slf4j" % "jcl-over-slf4j" % "1.7.25" % Test

lazy val scalaMock = "org.scalamock" %% "scalamock" % "4.0.0" % Test

/*
  This is needed by the processes started by MiniAccumuloCluster, which apparently
  touch the internals of Log4J 1.2 and not just the API. I'm looking at you, ZooKeeper. >:-(
 */
lazy val log4j1 = "log4j" % "log4j" % "1.2.17" % Test


lazy val core = (project in file("cumulative-core")).settings(
  name := "cumulative-core",
  autoScalaLibrary := true,
  libraryDependencies := Seq(
    accumuloCore,
    miniAccumuloCluster,
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
  crossScalaVersions := Seq("2.10.7", "2.11.12", "2.12.4")
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
  crossScalaVersions := Seq("2.10.7", "2.11.12", "2.12.4")
).dependsOn(core)

