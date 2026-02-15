import sbt.*

object Dependencies {

  object CatsEffect {
    private val catsLogsVersion = "2.7.0"
    private val core = "org.typelevel" %% "cats-core" % "2.12.0"
    private val catsEffect = "org.typelevel" %% "cats-effect" % "3.5.4"
    private val catsLogs = "org.typelevel" %% "log4cats-core" % catsLogsVersion
    private val catsLog4j = "org.typelevel" %% "log4cats-slf4j" % catsLogsVersion
    private val pureconfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.17.6"
    val catsTest = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
    val all: Seq[ModuleID] = Seq(core, catsEffect, catsLogs, catsLog4j, pureconfigCats)

  }

  object Logging {
    val slf4j = "org.slf4j" % "slf4j-api" % "1.7.25"
    val scalaLogging = ("com.typesafe.scala-logging" %% "scala-logging" % "3.9.5").cross(CrossVersion.for3Use2_13)
    val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.3.14"
    val logstashLogbackEncoder = "net.logstash.logback" % "logstash-logback-encoder" % "5.0"

    val all: Seq[ModuleID] = Seq(slf4j, scalaLogging, logbackClassic, logstashLogbackEncoder)
  }

  object Prometheus {
    private val prometheusSimpleClientV = "0.16.0"

    val all: Seq[ModuleID] = Seq(
      "me.dinowernli" % "java-grpc-prometheus" % "0.6.0" excludeAll "io.grpc",
      "io.prometheus" % "simpleclient_httpserver" % prometheusSimpleClientV,
      "io.prometheus" % "simpleclient_hotspot" % prometheusSimpleClientV
    )
  }

  object Http4s {
    val Http4sVersion = "0.23.27"
    val smithy4sVersion = "0.19.0-41-91762fb"
    val all: Seq[ModuleID] = Seq(
      "org.http4s" %% "http4s-ember-server" % Http4sVersion,
      "org.http4s" %% "http4s-ember-client" % Http4sVersion,
      "org.http4s" %% "http4s-circe" % Http4sVersion,
      "org.http4s" %% "http4s-dsl" % Http4sVersion,
      "com.disneystreaming.smithy4s" %% "smithy4s-http4s" % smithy4sVersion excludeAll (
        ExclusionRule("org.scala-lang.modules")
        ),
      "com.disneystreaming.smithy4s" %% "smithy4s-http4s-swagger" % smithy4sVersion excludeAll (
        ExclusionRule("org.scala-lang.modules")
        )
    )
  }

  object Fs2 {
    private val fs2Streams = "co.fs2" %% "fs2-core" % "3.10.2"

    val all: Seq[ModuleID] = Seq(fs2Streams)
  }

  object Testing {
    private val scalaTest = "org.scalatest" %% "scalatest" % "3.2.18" % Test

    val all: Seq[ModuleID] = Seq(
      scalaTest,
      CatsEffect.catsTest
    )
  }

  object Scala {
    val version = "3.4.2"
  }
}
