import sbt.*

object Dependencies {

  object CatsEffect {
    private val catsLogsVersion = "2.7.0"
    private val core           = "org.typelevel"         %% "cats-core"              % "2.12.0"
    private val catsEffect     = "org.typelevel"         %% "cats-effect"            % "3.5.4"
    private val catsLogs       = "org.typelevel"         %% "log4cats-core"          % catsLogsVersion
    private val catsLog4j      = "org.typelevel"         %% "log4cats-slf4j"         % catsLogsVersion
    private val pureconfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.17.6"
    val catsTest        = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
    val all: Seq[ModuleID] = Seq(core, catsEffect, catsLogs, catsLog4j, pureconfigCats)

  }

  object Prometheus {
    private val prometheusSimpleClientV = "0.16.0"

    val all: Seq[ModuleID] = Seq(
      "me.dinowernli" % "java-grpc-prometheus"    % "0.6.0" excludeAll "io.grpc",
      "io.prometheus" % "simpleclient_httpserver" % prometheusSimpleClientV,
      "io.prometheus" % "simpleclient_hotspot"    % prometheusSimpleClientV
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
