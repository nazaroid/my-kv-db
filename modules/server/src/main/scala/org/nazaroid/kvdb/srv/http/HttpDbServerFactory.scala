package org.nazaroid.kvdb.srv.http

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.IORuntime
import org.nazaroid.kvdb.algebra.{DbServer, DbSrvConf}
import org.nazaroid.kvdb.srv.composition.DiContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext

final class HttpDbServerFactory {

  def create(conf: DbSrvConf): IO[DbServer[IO]] = {
    implicit val runtime: IORuntime = IORuntime.builder().build()
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
    val di = new DiContainer[IO]
    Dispatcher.parallel[IO] use { implicit d: Dispatcher[IO] =>
      di.resolveDbServer(conf)
    }
  }
}
