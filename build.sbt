import BuildSettings.*
import Dependencies.*

val GLOBAL_VERSION = "25.2.1.0"

lazy val root = (project in file("."))
  .aggregate(
    `server`,
    `prometheus`
  )
  .settings(
    commonSettings,
    name                                   := "kvdb",
    version                                := GLOBAL_VERSION,
    Compile / unmanagedSourceDirectories   := Nil,
    Compile / unmanagedResourceDirectories := Nil,
    Test / unmanagedSourceDirectories      := Nil,
    Test / unmanagedResourceDirectories    := Nil,
    sbt.Keys.`package`                     := target.value,
    publish                                := {},
    publishLocal                           := {}
  )

lazy val `prometheus` = (project in file("modules/prometheus"))
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "prometheus",
    version := GLOBAL_VERSION,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/prometheus/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/prometheus/integration-tests-html-report")
  )

lazy val `server` = (project in file("modules/server"))
  .dependsOn(`prometheus` % "compile->compile;test->test;it->it")
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "server",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Http4s.all ++ Logging.all ++ Prometheus.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/server/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/server/integration-tests-html-report"),
    dockerBaseImage      := "openjdk:25-oraclelinux8",
    Docker / packageName := "org/nazaroid/kvdb/server",
    Docker / version     := version.value,
    envVars += (sys.props.get("config") match {
      case Some(confName: String) => "config" -> confName
      case _                      => "config" -> "application.conf"
    })
  )
