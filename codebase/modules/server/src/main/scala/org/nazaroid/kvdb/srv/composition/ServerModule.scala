package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.effect.Async
import cats.effect.implicits.given
import cats.effect.kernel.Resource
import fs2.io.file.Files
import fs2.io.net.Network
import org.nazaroid.kvdb.bitcask.{BitcaskEngine, BitcaskStatisticsService}
import org.nazaroid.kvdb.core.Server
import org.nazaroid.kvdb.srv.ServerConfig
import org.nazaroid.kvdb.srv.http.HttpServer
import org.typelevel.log4cats.Logger

final class ServerModule[F[_]: Async: Files: Logger: Parallel: Network](commonModule: CommonModule[F]) {
  private val config = commonModule.config

  def resolve: Resource[F, Server[F]] = {
    for {
      engine <- BitcaskEngine.init[F](config.engine)
      statisticsService <- BitcaskStatisticsService
        .create[F](
          engine.dbManager,
          org.nazaroid.kvdb.core.MonitoringConfig(),
          io.prometheus.client.CollectorRegistry.defaultRegistry
        )
        .toResource
    } yield {
      config.server match {
        case httpConf: ServerConfig.Http =>
          new org.nazaroid.kvdb.srv.http.HttpServer[F](httpConf, engine, statisticsService)
        case _: ServerConfig.Grpc => throw NotImplementedError(f"'GRPC' - server not supported yet")
      }
    }
  }
}
