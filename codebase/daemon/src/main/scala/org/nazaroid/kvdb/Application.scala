package org.nazaroid.kvdb

import cats.Parallel
import cats.effect.std.Dispatcher
import cats.effect.{Async, IO, IOApp}
import cats.implicits.*
import com.typesafe.config.ConfigFactory
import fs2.io.file.Files
import fs2.io.net.Network
import metrics.MetricExporter
import org.nazaroid.kvdb.srv.DbInstance
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax.given

final class Application[F[_]: Async: Files: Parallel: Network](using Dispatcher[F]) {

  // noinspection ScalaUnusedSymbol
  def start(): F[Unit] =
    Slf4jLogger.create[F] >>= { logger =>
      given Logger[F] = logger

      for {
        confName <- scala.util.Properties.envOrElse("config", "application.conf").pure[F]
        appConfig <- ConfigSource
          .fromConfig(ConfigFactory.load(confName).getConfig(AppConfig.appName))
          .loadF[F, AppConfig]()
        _ <- if (appConfig.metricsEnabled) new MetricExporter(appConfig.metricsPort).start() else ().pure[F]
        _ <- DbInstance[F]().resource(appConfig.dbConf).use(_ => Async[F].never)
      } yield ()
    }
}

object Main extends IOApp.Simple {

  override def run: IO[Unit] =
    Dispatcher.parallel[IO] use { d =>
      given Dispatcher[IO] = d

      new Application[IO]().start()
    }
}
