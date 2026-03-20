import BuildSettings.*
import Dependencies.*

lazy val `database` = (project in file("."))
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
