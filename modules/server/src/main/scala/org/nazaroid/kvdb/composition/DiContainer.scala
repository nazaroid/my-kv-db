package org.nazaroid.kvdb.composition

import cats.Parallel
import cats.data.Kleisli
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.effect.std.Dispatcher
import cats.implicits.*
import org.nazaroid.kvdb.AppConfig
import org.nazaroid.kvdb.algebra.DbServer
import org.typelevel.log4cats.Logger

class DiContainer[F[_]: Async: Logger: Parallel] {

  private val appState =
    AppState(
      Ref.unsafe("")
    )

  def resolveDbServer(
    appConfig: AppConfig
  )(implicit
    d: Dispatcher[F]
  ): F[DbServer[F]] = {
    commonModuleK >>> {
      for {
        dbServer <- dbServerK
      } yield dbServer
    }
  }.run(appConfig)

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
  ): Kleisli[F, AppConfig, CommonModule[F]] =
    Kleisli { (appConfig: AppConfig) =>
      {
        new CommonModule(appConfig, appState, d).pure[F]
      }

    }
}
