package org.nazaroid.kvdb

import cats.effect.implicits.given
import cats.effect.std.Dispatcher
import cats.effect.{Deferred, IO}
import fs2.io.net.Network
import org.nazaroid.kvdb.srv.DbRuntime
import org.nazaroid.kvdb.srv.composition.DiContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

final class DbInstance(val rt: DbRuntime = new DbRuntime()) {

  def runSync(conf: DbInstanceConfig): Unit = start(conf).unsafeRunSync()(rt.io)

  def runAsync(conf: DbInstanceConfig): Unit = start(conf).unsafeRunAndForget()(rt.io)

  // TODO: возвращать ресурс из resolveDbServer
  private def start(conf: DbInstanceConfig): IO[Unit] = {
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

    val di = new DiContainer[IO]
    Dispatcher.parallel[IO] use { implicit d: Dispatcher[IO] =>
      di
        .resolveDbServer(conf)
        .flatMap { srv =>
          for {
            stopSignal <- Deferred[IO, Unit]
            _ = rt
              .stopRef
              .set(() => stopSignal.complete(()).map(_ => ()).unsafeRunSync()(rt.io))
            _ <- srv.run(stopSignal)
          } yield ()
        }
    }
  }

}
