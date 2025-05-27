package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.data.Kleisli
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.effect.std.Dispatcher
import cats.implicits.*
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.DbServer
import org.nazaroid.kvdb.srv.{DbSrvConf, DbSrvState}
import org.typelevel.log4cats.Logger

class DiContainer[F[_]: Async: Logger: Parallel: Network] {

  private val state =
    DbSrvState(
      Ref.unsafe("")
    )

  def resolveDbServer(
    conf: DbSrvConf
  )(implicit
    d: Dispatcher[F]
  ): F[DbServer[F]] = {
    commonModuleK >>> {
      for {
        dbServer <- dbServerK
      } yield dbServer
    }
  }.run(conf)

  private def dbServerK(
    implicit
    d: Dispatcher[F]
  ): Kleisli[F, CommonModule[F], DbServer[F]] =
    Kleisli { commonModule =>
      new DbServerModule(commonModule).resolve
    }

  private def commonModuleK(
    implicit
    d: Dispatcher[F]
  ): Kleisli[F, DbSrvConf, CommonModule[F]] =
    Kleisli { (conf: DbSrvConf) =>
      {
        new CommonModule(conf, state, d).pure[F]
      }

    }
}
