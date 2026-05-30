import BuildSettings.*
import Dependencies.*

val GLOBAL_VERSION = "25.2.1.0"

lazy val root = (project in file("."))
  .aggregate(
    `daemon`,
    `metrics`,
    `bin-file-io`,
    `bitcask`,
    `core`,
    `server`
  )
  .disablePlugins(AssemblyPlugin)
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

lazy val `metrics` = (project in file("codebase/modules/utils/metrics"))
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "metrics",
    version := GLOBAL_VERSION,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/metrics/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/metrics/integration-tests-html-report")
  )

lazy val `bin-file-io` = (project in file("codebase/modules/bin-file-io"))
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "bin-file-io",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Logging.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/bin-file-io/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/bin-file-io/integration-tests-html-report")
  )

lazy val `bitcask` = (project in file("codebase/modules/bitcask"))
  .dependsOn(`bin-file-io` % "compile->compile;test->test;it->it")
  .dependsOn(`core` % "compile->compile;test->test;it->it")
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "bitcask",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Logging.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/bitcask/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/bitcask/integration-tests-html-report")
  )

lazy val `core` = (project in file("codebase/modules/core"))
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "core",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Logging.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/database/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/database/integration-tests-html-report")
  )

lazy val `server` = (project in file("codebase/modules/server"))
  .dependsOn(`metrics` % "compile->compile;test->test;it->it")
  .dependsOn(`bitcask` % "compile->compile;test->test;it->it")
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
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/server/integration-tests-html-report")
  )

lazy val `daemon` = (project in file("codebase/daemon"))
  .dependsOn(`metrics` % "compile->compile;test->test;it->it")
  .dependsOn(`server` % "compile->compile;test->test;it->it")
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .configs(Integration)
  .settings(
    commonSettings,
    assemblyMergeSetting,
    name    := "daemon",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Http4s.all ++ Logging.all ++ Prometheus.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/daemon/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/daemon/integration-tests-html-report"),
    dockerBaseImage      := "amazoncorretto:17-al2023 ",
    Docker / packageName := "org/nazaroid/kvdb/server",
    Docker / version     := version.value,
    envVars += (sys.props.get("config") match {
      case Some(confName: String) => "config" -> confName
      case _                      => "config" -> "application.conf"
    })
  )
