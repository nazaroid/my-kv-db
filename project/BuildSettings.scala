import Dependencies.*
import com.typesafe.sbt.SbtNativePackager.Docker
import sbt.Keys.*
import sbt.{Def, *}
import sbtassembly.AssemblyKeys.*
import sbtassembly.AssemblyPlugin.autoImport.ShadeRule
import sbtassembly.{MergeStrategy, PathList}

object BuildSettings {
  val javaVersion = "1.8"

  lazy val Integration     = config("it") extend Test
  lazy val publishSnapshot = taskKey[Unit]("Custom docker:publish")
  lazy val publishRelease  = taskKey[Unit]("Custom docker:publish")

  def dockerPublishConditional(isProd: Boolean): Def.Initialize[Task[Unit]] =
    Def.taskDyn {
      (isProd, isSnapshot.value) match {
        case (false, false) =>
          throw new IllegalStateException(
            "Please check GLOBAL_VERSION. It should be like ***.***-SNAPSHOT"
          )
        case (false, true) => Docker / publish
        case (true, false) => Docker / publish
        case (true, true) =>
          throw new IllegalStateException(
            "Please check GLOBAL_VERSION. It should not contain SNAPSHOT postfix"
          )
      }
    }

  lazy val commonSettings: Seq[Setting[?]] = Seq(
    organization := s"org.nazaroid.db",
    libraryDependencies ++= CatsEffect.all ++ Fs2.all ++ Http4s.all ++ Prometheus.all ++ Testing.all,
    scalaVersion := Dependencies.Scala.version,
    javacOptions ++= Seq("-source", javaVersion, "-target", javaVersion),
    updateOptions                       := updateOptions.value.withGigahorse(false),
    publishSnapshot                     := dockerPublishConditional(false).value,
    publishRelease                      := dockerPublishConditional(true).value,
    update / logLevel                   := Level.Warn,
    Global / cancelable                 := true,
    Test / parallelExecution            := false,
    Integration / parallelExecution     := false,
    IntegrationTest / parallelExecution := false,
    Test / fork                         := true,
    Integration / fork                  := true,
    IntegrationTest / fork              := true,
    Compile / fork                      := true,
    javaOptions += "-Djava.net.preferIPv4Stack=true",
    assembly / test := {},
    scalacOptions ++= Seq(
      "-Wunused:imports",
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-Xfatal-warnings",
      "-deprecation",
      "-feature",
      "-language:postfixOps",
      "-encoding",
      "UTF-8",
      "-unchecked",
      "-Xmax-inlines",
      "200"
    ),
    semanticdbEnabled := true
  )

  lazy val assemblyMergeSetting = Seq(
    assembly / assemblyMergeStrategy := {
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.discard
      case x => MergeStrategy.defaultMergeStrategy(x)
    }
  )
}
