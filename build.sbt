ThisBuild / scalaVersion := "2.12.8"
ThisBuild / organization := "org.sancio"

lazy val kafkaFile = (project in file("."))
  .settings(
    name := "kafka-file",
    libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2",
    libraryDependencies += "commons-codec" % "commons-codec" % "1.11",
    libraryDependencies += "commons-io" % "commons-io" % "2.6",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.1.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  )
