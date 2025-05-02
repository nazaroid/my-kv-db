package org.nazaroid.kvdb

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.IORuntime
import org.nazaroid.kvdb.algebra.DbServer
import org.nazaroid.kvdb.composition.DiContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext

final class TestDbServerFactory {
  private implicit val runtime: IORuntime = IORuntime.builder().build()
  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
  private val di = new DiContainer[IO]

  def create(appConfig: AppConfig): IO[DbServer[IO]] = {
    Dispatcher.parallel[IO] use { implicit d: Dispatcher[IO] =>
      di.resolveDbServer(appConfig)
    }
  }
}