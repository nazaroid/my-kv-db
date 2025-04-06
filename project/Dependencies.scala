import sbt.*

object Dependencies {


  object CatsEffect {
    private val catsLogsVersion = "2.7.0"

    val core = "org.typelevel" %% "cats-core" % "2.12.0"
    val catsEffect = "org.typelevel" %% "cats-effect" % "3.5.4"
    val catsLogs = "org.typelevel" %% "log4cats-core" % catsLogsVersion
    val catsLog4j = "org.typelevel" %% "log4cats-slf4j" % catsLogsVersion
    val pureconfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.17.6"
    val all: Seq[ModuleID] = Seq(core, catsEffect, catsLogs, catsLog4j, pureconfigCats)
    val catsTest = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
  }

  object Testing {
    val scalaTest = "org.scalatest" %% "scalatest" % "3.2.18" % Test

    val all: Seq[ModuleID] = Seq(
      testContainers,
      scalaTest,
      CatsEffect.catsTest
    )
  }

  object Scala {
    val version = "3.4.2"
  }
}
