package org.nazaroid.kvdb

import cats.Parallel
import cats.effect.std.Dispatcher
import cats.effect.{Async, IO, IOApp}
import cats.implicits.*
import com.typesafe.config.ConfigFactory
import org.nazaroid.kvdb.composition.DiContainer
import org.nazaroid.kvdb.utils.metrics.MetricExporter
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax.*

final class Application[F[_]: Async: Parallel](implicit d: Dispatcher[F]) {

  // noinspection ScalaUnusedSymbol
  def start(): F[Unit] =
    Slf4jLogger.create[F] >>= { implicit logger =>
      for {
        confName <- scala.util.Properties.envOrElse("config", "application.conf").pure[F]
        appConfig <- ConfigSource
          .fromConfig(ConfigFactory.load(confName).getConfig(AppConfig.appName))
          .loadF[F, AppConfig]()
        _     <- if (appConfig.metricsEnabled) new MetricExporter(appConfig.metricsPort).start() else ().pure[F]
        di    <- new DiContainer[F]().pure[F]
        dbSrb <- di.resolveDbServer(appConfig.dbSrvConf)
        _ <- fs2
          .Stream
          .emits(Seq(dbSrb.run()))
          .parEvalMapUnbounded(identity)
          .compile
          .drain
      } yield ()
    }
}

object Main extends IOApp.Simple {

  override def run: IO[Unit] =
    Dispatcher.parallel[IO] use { implicit d =>
      new Application[IO].start()
    }
}
