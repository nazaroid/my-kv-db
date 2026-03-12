ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "statistics",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.5.4",
      "co.fs2" %% "fs2-io" % "3.10.0",
      "org.typelevel" %% "log4cats-slf4j" % "2.7.0",
      "io.prometheus" % "simpleclient" % "0.16.0",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )
