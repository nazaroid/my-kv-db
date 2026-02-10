package org.nazaroid.kvdb

import cats.effect.IO
import cats.effect.implicits.given
import cats.effect.kernel.Deferred
import cats.effect.std.Dispatcher
import fs2.io.net.Network
import org.nazaroid.kvdb.srv.DbRuntime
import org.nazaroid.kvdb.srv.composition.DiContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

final class DbInstance(val rt: DbRuntime = new DbRuntime()) {

  def runSync(conf: DbInstanceConfig): Unit = start(conf).unsafeRunSync()(rt.io)

  def runAsync(conf: DbInstanceConfig): Unit = start(conf).unsafeRunAndForget()(rt.io)

  private def start(conf: DbInstanceConfig): IO[Unit] = {
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

    val di = new DiContainer[IO]
    Dispatcher.parallel[IO] use { implicit d: Dispatcher[IO] =>
      for {
        stopSignal <- Deferred[IO, Unit]
        _ = rt
          .stopActionRef
          .set(() => stopSignal.complete(()).map(_ => ()).unsafeRunSync()(rt.io))
        _          <- Logger[IO].info("server starting...")
        runningSrv <- di.resolveServer(conf).map(_.flatMap(_.run()))
        _          <- runningSrv.use(_ => stopSignal.get >> Logger[IO].warn("server shutting down..."))
      } yield ()
    }
  }
}
