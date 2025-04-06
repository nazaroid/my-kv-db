import BuildSettings.*
import Dependencies.*
import git.plugin.CustomGit.gitVersionSettings

val GLOBAL_VERSION = "25.2.1.0"

lazy val root = (project in file("."))
  .aggregate(
    `server`
  )
  .settings(
    commonSettings,
    name := "server",
    version := GLOBAL_VERSION,
    Compile / unmanagedSourceDirectories := Nil,
    Compile / unmanagedResourceDirectories := Nil,
    Test / unmanagedSourceDirectories := Nil,
    Test / unmanagedResourceDirectories := Nil,
    sbt.Keys.`package` := target.value,
    publish := {},
    publishLocal := {}
  )

lazy val `server` = (project in file("modules/server"))
  .dependsOn(`cust-cat-api-common` % "compile->compile;test->test;it->it")
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(AssemblyPlugin)
  .configs(Integration)
  .settings(
    commonSettings,
    name := "server",
    version := GLOBAL_VERSION,
    libraryDependencies ++= CatsEffect.all ++ Testing.all,
    excludeDependencies -= ExclusionRule("log4j", "log4j"),
    gitVersionSettings(filename = "git.properties"),
    Test / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/server/unit-tests-html-report"),
    inConfig(Integration)(Defaults.testSettings),
    Integration / testOptions += Tests
      .Argument(TestFrameworks.ScalaTest, "-u", "scalatest/server/integration-tests-html-report"),
    dockerBaseImage := "openjdk:25-oraclelinux8",
    Docker / packageName := "ser/customer-catalog/server",
    Docker / version := version.value,
    envVars += (sys.props.get("config") match {
      case Some(confName: String) => "config" -> confName
      case _                      => "config" -> "application.conf"
    })
  )
