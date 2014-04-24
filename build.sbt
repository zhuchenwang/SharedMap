import sbt._
import Keys._

name := "pipeline"

version := "1.0"

scalaVersion := "2.10.3"

fork in ThisBuild := false

parallelExecution in ThisBuild := false

javaOptions in ThisBuild ++= Seq("-Xmx2048m", "-XX:MaxPermSize=512m")

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "spray repo" at "http://repo.spray.io"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.2",
  "io.spray" % "spray-can" % "1.3.1",
  "io.spray" % "spray-http" % "1.3.1",
  "org.scalatest" %% "scalatest" % "2.1.0" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.2" % "test"
)