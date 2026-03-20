import BuildSettings.*
import Dependencies.*

val GLOBAL_VERSION = "25.2.1.0"

lazy val root = (project in file("."))
  .aggregate(
    `service`,
    `metrics`,
    `prometheus`,
    `bin-file-io`,
    `bitcask`,
    `database`,
    `statistics`,
    `server`
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

lazy val `prometheus` = (project in file("codebase/modules/utils/third_party/prometheus"))
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

lazy val `metrics` = (project in file("codebase/modules/utils/metrics"))
  .dependsOn(`prometheus` % "compile->compile;test->test;it->it")
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

lazy val `database` = (project in file("codebase/modules/database"))
  .dependsOn(`bitcask` % "compile->compile;test->test;it->it")
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "database",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Logging.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/database/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/database/integration-tests-html-report")
  )

lazy val `statistics` = (project in file("codebase/modules/statistics"))
  .dependsOn(`database` % "compile->compile;test->test;it->it")
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "statistics",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Logging.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/statistics/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/statistics/integration-tests-html-report")
  )

lazy val `server` = (project in file("codebase/modules/server"))
  .dependsOn(`metrics` % "compile->compile;test->test;it->it")
  .dependsOn(`statistics` % "compile->compile;test->test;it->it")
  .dependsOn(`database` % "compile->compile;test->test;it->it")
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

lazy val `service` = (project in file("codebase/service"))
  .dependsOn(`metrics` % "compile->compile;test->test;it->it")
  .dependsOn(`server` % "compile->compile;test->test;it->it")
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name    := "service",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Http4s.all ++ Logging.all ++ Prometheus.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/service/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/service/integration-tests-html-report"),
    dockerBaseImage      := "openjdk:25-oraclelinux8",
    Docker / packageName := "org/nazaroid/kvdb/server",
    Docker / version     := version.value,
    envVars += (sys.props.get("config") match {
      case Some(confName: String) => "config" -> confName
      case _                      => "config" -> "application.conf"
    })
  )
