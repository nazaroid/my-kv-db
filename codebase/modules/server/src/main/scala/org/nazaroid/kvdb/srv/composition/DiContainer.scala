package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.data.Kleisli
import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.implicits.*
import fs2.io.file.Files
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.Server
import org.nazaroid.kvdb.DbInstanceConfig
import org.typelevel.log4cats.Logger

class DiContainer[F[_]: Async: Files: Logger: Parallel: Network] {

  def resolveDbServer(conf: DbInstanceConfig)(implicit d: Dispatcher[F]): F[Server[F]] = {
    commonModuleK >>> {
      for {
        dbServer <- dbServerK
      } yield dbServer
    }
  }.run(conf)

  private def dbServerK(implicit d: Dispatcher[F]): Kleisli[F, CommonModule[F], Server[F]] =
    Kleisli { commonModule =>
      new ServerModule(commonModule).resolve
    }

  private def commonModuleK(
    implicit
    d: Dispatcher[F]
  ): Kleisli[F, DbInstanceConfig, CommonModule[F]] =
    Kleisli { (conf: DbInstanceConfig) => { new CommonModule(conf, d).pure[F] } }
}
