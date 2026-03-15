package org.nazaroid.kvdb.statistics

import cats.effect.{Async, Resource}
import cats.implicits.given
import org.nazaroid.kvdb.bitcask.storage.StorageManager
import org.nazaroid.kvdb.bitcask.storage.StorageManager
import org.typelevel.log4cats.Logger

/** Integration layer for statistics with BitcaskEngine */
class StatisticsIntegration[F[_]: Async: Logger](
  statisticsService: StatisticsService[F]
) {

  /** Start background monitoring */
  def startMonitoring(): F[Unit] = {
    Logger[F].info("Starting statistics integration monitoring")
    statisticsService.startMonitoring()
  }

  /** Stop background monitoring */
  def stopMonitoring(): F[Unit] = {
    Logger[F].info("Stopping statistics integration monitoring")
    statisticsService.stopMonitoring()
  }

  /** Get all databases with their statistics */
  def getAllDatabases: F[List[DatabaseInfo]] = {
    statisticsService.getDatabases
  }

  /** Get specific database statistics */
  def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] = {
    statisticsService.getDatabaseStats(dbName)
  }

  /** Get segment statistics for a database */
  def getSegmentStats(dbName: String): F[List[SegmentInfo]] = {
    statisticsService.getSegmentStats(dbName)
  }
  
  /** Get storage statistics (delegates to storageManager) */
  def getStats: F[org.nazaroid.kvdb.bitcask.storage.DatabaseStats] = {
    statisticsService.getStats
  }

  /** Get health check information */
  def getHealthCheck: F[HealthStatus] = {
    for {
      databases <- statisticsService.getDatabases
      totalDbs = databases.size
      healthyDbs = databases.count(_.fragmentationRatio < 0.5)
      avgFragmentation = if (totalDbs > 0) {
        databases.map(_.fragmentationRatio).sum / totalDbs
      } else 0.0
      
      healthStatus = if (healthyDbs == totalDbs && avgFragmentation < 0.3) {
        HealthStatus.Healthy
      } else if (healthyDbs > 0) {
        HealthStatus.Degraded
      } else {
        HealthStatus.Unhealthy
      }
      
    } yield HealthStatus(
      status = healthStatus,
      totalDatabases = totalDbs,
      healthyDatabases = healthyDbs,
      averageFragmentation = avgFragmentation,
      timestamp = System.currentTimeMillis()
    )
  }
}

case class HealthStatus(
  status: HealthStatus.Status,
  totalDatabases: Int,
  healthyDatabases: Int,
  averageFragmentation: Double,
  timestamp: Long
)

object HealthStatus {
  sealed trait Status
  case object Healthy extends Status
  case object Degraded extends Status  
  case object Unhealthy extends Status
}

object StatisticsIntegration {
  def create[F[_]: Async: Logger](
    storageManager: StorageManager[F],
    config: MonitoringConfig = MonitoringConfig()
  ): F[StatisticsIntegration[F]] = {
    // Create with default NoOp adapter
    createWithAdapter(storageManager, config, MetricsAdapter.createNoOpAdapter())
  }
  
  def createWithPrometheus[F[_]: Async: Logger](
    storageManager: StorageManager[F],
    config: MonitoringConfig = MonitoringConfig(),
    collectorRegistry: io.prometheus.client.CollectorRegistry
  ): F[StatisticsIntegration[F]] = {
    val prometheusAdapter = MetricsAdapter.createPrometheusAdapter(collectorRegistry)
    for {
      integration <- createWithAdapter(storageManager, config, prometheusAdapter)
    } yield integration
  }
  
  def createWithAdapter[F[_]: Async: Logger](
    storageManager: StorageManager[F],
    config: MonitoringConfig,
    metricsAdapter: MetricsAdapter[F]
  ): F[StatisticsIntegration[F]] = {
    for {
      statisticsService <- StatisticsService.createWithAdapter(storageManager, config, metricsAdapter)
      integration = new StatisticsIntegration(statisticsService)
    } yield integration
  }
}
