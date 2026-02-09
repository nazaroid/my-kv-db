package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.data.Kleisli
import cats.effect.std.Dispatcher
import cats.effect.{Async, Resource}
import cats.implicits.*
import fs2.io.file.Files
import fs2.io.net.Network
import org.nazaroid.kvdb.DbInstanceConfig
import org.nazaroid.kvdb.algebra.Server
import org.typelevel.log4cats.Logger

class DiContainer[F[_]: Async: Files: Logger: Parallel: Network] {

  def resolveServer(conf: DbInstanceConfig)(implicit d: Dispatcher[F]): F[Resource[F, Server[F]]] = {
    commonModuleK >>> {
      for {
        server <- serverK
      } yield server
    }
  }.run(conf)

  private def serverK(implicit d: Dispatcher[F]): Kleisli[F, CommonModule[F], Resource[F, Server[F]]] =
    Kleisli { commonModule => ServerModule(commonModule).resolve.pure[F] }

  private def commonModuleK(
    implicit
    d: Dispatcher[F]
  ): Kleisli[F, DbInstanceConfig, CommonModule[F]] =
    Kleisli { (conf: DbInstanceConfig) => { new CommonModule(conf, d).pure[F] } }
}
