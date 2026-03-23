package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.effect.Async
import cats.effect.kernel.Resource
import fs2.io.file.Files
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.Server
import org.nazaroid.kvdb.bitkask.{BitcaskEngine, BitcaskDatabaseManager}
import org.nazaroid.kvdb.srv.ServerConfig
import org.nazaroid.kvdb.srv.http.HttpServer
import org.typelevel.log4cats.Logger

final class ServerModule[F[_]: Async: Files: Logger: Parallel: Network](commonModule: CommonModule[F]) {
  private val config = commonModule.config

  def resolve: Resource[F, Server[F]] = {
    for {
      databaseManager <- BitcaskDatabaseManager.create[F](config.engine.rootDir)
      engine = new BitcaskEngine[F](databaseManager)
      statisticsService <- org.nazaroid.kvdb.statistics.StatisticsService.createWithPrometheus(
        databaseManager,
        org.nazaroid.kvdb.statistics.MonitoringConfig(),
        new io.prometheus.client.CollectorRegistry()
      )
    } yield {
      config.server match {
        case httpConf: ServerConfig.Http => 
          new org.nazaroid.kvdb.srv.http.HttpServer[F](httpConf, engine, statisticsService)
        case _: ServerConfig.Grpc        => throw NotImplementedError(f"'GRPC' - server not supported yet")
      }
    }
  }
}
