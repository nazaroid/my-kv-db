package org.nazaroid.kvdb.bitcask

import cats.effect.Async
import cats.implicits.given
import fs2.io.file.Files
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.core.{DatabaseManager, MetricsAdapter, MonitoringConfig, StatisticsService}
import org.typelevel.log4cats.Logger

object BitcaskStatisticsService {

  def create[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:          MonitoringConfig = MonitoringConfig(),
    reg:             CollectorRegistry = CollectorRegistry.defaultRegistry
  ): F[StatisticsService[F]] = {
    StatisticsService.createWithAdapter(
      databaseManager,
      config,
      BitcaskPrometheusMetricsAdapter.create(reg).map(_.asInstanceOf[MetricsAdapter[F]])
    )
  }

  def create[F[_]: Async: Files: Logger](
    config: MonitoringConfig = MonitoringConfig(),
    reg:    CollectorRegistry = CollectorRegistry.defaultRegistry
  ): F[BitcaskStatisticsService[F]] = {
    for {
      metricsAdapter <- BitcaskPrometheusMetricsAdapter.create[F](reg)
      service = new BitcaskStatisticsService[F](metricsAdapter)
    } yield service
  }
}

class BitcaskStatisticsService[F[_]: Async: Files: Logger](
  val metricsAdapter: BitcaskPrometheusMetricsAdapter[F]
) {

  def registerMetrics(databaseManager: DatabaseManager[F]): F[Unit] = {
    StatisticsService.createWithAdapter(
      databaseManager,
      MonitoringConfig(),
      metricsAdapter.asInstanceOf[MetricsAdapter[F]]
    ).flatMap(_.registerMetrics())
  }

  def startMonitoring(): F[Unit] = {
    StatisticsService.createWithAdapter(
      null, // Will be set later
      MonitoringConfig(),
      metricsAdapter.asInstanceOf[MetricsAdapter[F]]
    ).flatMap(_.startMonitoring())
  }
}
