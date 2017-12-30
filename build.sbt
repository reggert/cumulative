name := "cumulative"

organization in ThisBuild := "io.github.reggert.cumulative"

version in ThisBuild := "0.0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.4"

javacOptions in ThisBuild ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions in ThisBuild += "-target:jvm-1.8"

lazy val accumuloVersion = "1.8.1"

lazy val core = (project in file("cumulative-core")).settings(
  name := "cumulative-core",
  autoScalaLibrary := false,
  libraryDependencies := Seq(
    "org.apache.accumulo" % "accumulo-core" % accumuloVersion % Compile
      exclude("javax.activation", "activation")
      exclude("log4j", "log4j")
      exclude("javax.servlet", "servlet-api"),
    "org.scalatest" %% "scalatest" % "3.0.4" % Test
  )
)
