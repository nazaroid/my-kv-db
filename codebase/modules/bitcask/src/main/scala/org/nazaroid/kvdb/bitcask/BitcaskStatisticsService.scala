package org.nazaroid.kvdb.bitcask

import cats.effect.Async
import cats.implicits.given
import fs2.io.file.Files
import org.nazaroid.kvdb.core.{DatabaseManager, MetricsAdapter, MonitoringConfig, StatisticsService}
import org.typelevel.log4cats.Logger

object BitcaskStatisticsService {

  def create[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:          MonitoringConfig = MonitoringConfig()
  ): F[StatisticsService[F]] = {
    StatisticsService.createWithAdapter(
      databaseManager,
      config,
      BitcaskPrometheusMetricsAdapter.create.map(_.asInstanceOf[MetricsAdapter[F]])
    )
  }
}
