package org.nazaroid.kvdb.srv.http

import cats.effect.implicits.given
import cats.effect.std.Dispatcher
import cats.effect.unsafe.IORuntime
import cats.effect.{Async, IO}
import cats.implicits.given
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.{DbServerHandle, DbSrvConf}
import org.nazaroid.kvdb.srv.composition.DiContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

final class HttpDbServerFactory[F[_]: Async] {

  implicit val rt: DbRuntimeIO =
    new DbRuntimeIO(IORuntime.builder().build(), scala.concurrent.ExecutionContext.Implicits.global)

  def start(conf: DbSrvConf): F[DbServerHandle] = {
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

    val di = new DiContainer[IO]
    (Dispatcher.parallel[IO] use { implicit d: Dispatcher[IO] =>
      di
        .resolveDbServer(conf)
        .flatMap(_.run())
    }).unsafeRunSync()(rt.io).pure[F]
  }
}
