package com.uzumdata.cc.api.composition

import cats.Parallel
import cats.data.Kleisli
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.effect.std.Dispatcher
import cats.implicits.*
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.uzumdata.cc.api.algebra.{Engine, FeatureDbMigrator}
import com.uzumdata.cc.api.{AppConfig, AppMetrics}
import fs2.io.net.Network
import org.typelevel.log4cats.Logger

import java.util.concurrent.ConcurrentHashMap

// noinspection ScalaUnusedSymbol
class DiContainer[F[_]: Async: Logger: Parallel: Network] {

  private val appMetrics = new AppMetrics()

  private val appState =
    AppState(
      Ref.unsafe(""),
      Ref.unsafe(new ConcurrentHashMap[String, PreparedStatement]()),
      Ref.unsafe(Map.empty),
      Ref.unsafe(Map.empty)
    )

  def resolveEngine(
    appConfig: AppConfig
  )(implicit
    d: Dispatcher[F]
  ): F[Engine[F]] = {
    commonModuleK >>> {
      for {
        engine <- apiEngineK
      } yield engine
    }
  }.run(appConfig)

  private def apiEngineK(
    implicit
    d: Dispatcher[F]
  ): Kleisli[F, CommonModule[F], Engine[F]] =
    Kleisli { commonModule =>
      new ApiEngineModule(commonModule).resolve
    }

  def resolveScyllaMigrator(
    appConfig: AppConfig
  )(implicit
    d: Dispatcher[F]
  ): F[FeatureDbMigrator[F]] = {
    commonModuleK >>> {
      for {
        migrator <- scyllaMigratorK
      } yield migrator
    }
  }.run(appConfig)

  private def commonModuleK(
    implicit
    d: Dispatcher[F]
  ): Kleisli[F, AppConfig, CommonModule[F]] =
    Kleisli { (appConfig: AppConfig) =>
      {
        new CommonModule(appConfig, appState, appMetrics, d).pure[F]
      }

    }

  private def scyllaMigratorK(
    implicit
    d: Dispatcher[F]
  ): Kleisli[F, CommonModule[F], FeatureDbMigrator[F]] =
    Kleisli { commonModule =>
      new ScyllaMigratorModule(commonModule).resolve
    }
}
