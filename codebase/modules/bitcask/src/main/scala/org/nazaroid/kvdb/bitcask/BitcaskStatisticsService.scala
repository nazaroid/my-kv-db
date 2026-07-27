package org.nazaroid.kvdb.bitcask

import cats.effect.Async
import cats.implicits.given
import fs2.io.file.Files
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.core.{Catalog, CatalogMetricRecorder, MonitoringConfig, StatisticsService}
import org.typelevel.log4cats.Logger

object BitcaskStatisticsService {

  def create[F[_]: Async: Files: Logger](
    catalog: Catalog[F],
    config:  MonitoringConfig = MonitoringConfig(),
    reg:     CollectorRegistry = CollectorRegistry.defaultRegistry
  ): F[StatisticsService[F]] = {
    StatisticsService.createWithAdapter(
      catalog,
      config,
      BitcaskCatalogMetricRecorder.create(reg).map(_.asInstanceOf[CatalogMetricRecorder[F]])
    )
  }
}
