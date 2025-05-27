package org.nazaroid.kvdb.srv

import cats.effect.IO
import cats.effect.implicits.given
import cats.effect.std.Dispatcher
import fs2.io.net.Network
import org.nazaroid.kvdb.srv.composition.DiContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

final class DbServerFactory(val rt: DbRuntime = new DbRuntime()) {

  def runSync(conf: DbSrvConf): Unit = start(conf).unsafeRunSync()(rt.io)

  def runAsync(conf: DbSrvConf): Unit = start(conf).unsafeRunAndForget()(rt.io)

  private def start(conf: DbSrvConf): IO[Unit] = {
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

    val di = new DiContainer[IO]
    Dispatcher.parallel[IO] use { implicit d: Dispatcher[IO] =>
      di
        .resolveDbServer(conf)
        .flatMap(_.run())
    }
  }

}
