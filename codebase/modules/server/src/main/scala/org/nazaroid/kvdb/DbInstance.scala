package org.nazaroid.kvdb

import cats.Parallel
import cats.effect.kernel.{Deferred, Spawn}
import cats.effect.std.Dispatcher
import cats.effect.syntax.all.*
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import fs2.io.net.Network
import org.nazaroid.kvdb.srv.composition.DiContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger // для доступа к .start

final case class DbHandle[F[_]: Async: Files: Parallel: Network](stop: F[Unit])

final class DbInstance[F[_]: Async: Files: Parallel: Network: Spawn] {

  def resource(conf: DbInstanceConfig)(using d: Dispatcher[F]): Resource[F, DbHandle[F]] =
    for {
      given Logger[F] <- Resource.eval(Slf4jLogger.create[F])
      di = new DiContainer[F]
      stopSignal <- Resource.eval(Deferred[F, Unit])
      serverRes  <- Resource.eval(di.resolveServer(conf))
      _ <- Resource.eval(Logger[F].info("starting..."))
      _ <- serverRes.flatMap(_.run()).use(_ => stopSignal.get).background
    } yield DbHandle(
      stop = Logger[F].info("stopping...") *> stopSignal.complete(()).void
    )
}
