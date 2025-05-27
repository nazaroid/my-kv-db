package org.nazaroid.kvdb

import cats.Parallel
import cats.effect.std.Dispatcher
import cats.effect.{Async, IO, IOApp}
import cats.implicits.*
import com.typesafe.config.ConfigFactory
import fs2.io.net.Network
import org.nazaroid.kvdb.utils.metrics.MetricExporter
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax.*

final class Application[F[_]: Async: Parallel: Network](implicit d: Dispatcher[F]) {

  // noinspection ScalaUnusedSymbol
  def start(): F[Unit] =
    Slf4jLogger.create[F] >>= { implicit logger =>
      for {
        confName <- scala.util.Properties.envOrElse("config", "application.conf").pure[F]
        appConfig <- ConfigSource
          .fromConfig(ConfigFactory.load(confName).getConfig(AppConfig.appName))
          .loadF[F, AppConfig]()
        _ <- if (appConfig.metricsEnabled) new MetricExporter(appConfig.metricsPort).start() else ().pure[F]
        _ <- Async[F].blocking(new Db().runSync(appConfig.dbSrvConf))
      } yield ()
    }
}

object Main extends IOApp.Simple {

  override def run: IO[Unit] =
    Dispatcher.parallel[IO] use { implicit d =>
      new Application[IO].start()
    }
}
