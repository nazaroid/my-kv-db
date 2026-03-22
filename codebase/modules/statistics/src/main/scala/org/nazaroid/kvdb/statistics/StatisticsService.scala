package org.nazaroid.kvdb.statistics

import cats.effect.implicits.given
import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.io.file.{Files, Path}
import io.circe.*
import org.nazaroid.kvdb.core.{CatalogStats, DatabaseInfo, DatabaseManager, DatabaseStats, SegmentInfo, TableInfo}
import org.typelevel.log4cats.Logger

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Background process for monitoring segments and fragmentation */
trait StatisticsService[F[_]] {
  def startMonitoring():                F[Unit]
  def stopMonitoring():                 F[Unit]
  def getDatabases:                     F[List[DatabaseInfo]]
  def registerMetrics():                F[Unit]

  def getStats: F[CatalogStats]
  def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]]
  def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]]
}

case class MonitoringConfig(
  checkInterval:              FiniteDuration = 30.seconds,
  enableBackgroundMonitoring: Boolean = true,
  maxStaleRatio:              Double = 0.3,
  compactionThreshold:        Double = 0.5)

class StatisticsServiceImpl[F[_]: Async: Files: Logger](
  databaseManager: DatabaseManager[F],  // ✅ Работаем с базами, не с таблицами
  config:         MonitoringConfig,
  monitoringRef:  Ref[F, Boolean],
  metricsAdapter: MetricsAdapter[F] // Injected via constructor, no Option!
) extends StatisticsService[F] {

  override def registerMetrics(): F[Unit] = {
    for {
      _ <- Logger[F].info("Registering metrics with adapter")
      _ <- metricsAdapter.registerDatabaseMetrics()
      _ <- metricsAdapter.registerTableMetrics()
      _ <- metricsAdapter.registerSegmentMetrics()
      // Initial update with current values
      _ <- updateAdapterMetrics()
    } yield ()
  }

  override def startMonitoring(): F[Unit] = {
    if (config.enableBackgroundMonitoring) {
      // Define the monitoring stream recursively to ensure it restarts after errors
      lazy val monitoringStream: Stream[F, Unit] = Stream
        .fixedRate[F](config.checkInterval)
        .evalMap(_ => updateAdapterMetrics())
        .handleErrorWith { error =>
          // Log the error, wait for backoff, and restart the stream
          val recovery = Stream.eval(
            Logger[F].error(s"Error in monitoring stream: $error") *>
              Async[F].sleep(5.seconds)
          )
          recovery >> monitoringStream
        }

      for {
        _ <- Logger[F].info("Starting statistics monitoring service")
        _ <- monitoringRef.set(true)
        // .start runs the stream in a background Fiber to prevent blocking the startup
        _ <- monitoringStream.compile.drain.start.void
      } yield ()
    } else {
      Logger[F].info("Background monitoring disabled")
    }
  }

  override def stopMonitoring(): F[Unit] = {
    Logger[F].info("Stopping statistics monitoring service")
    monitoringRef.set(false)
  }
  /** Update metrics through adapter */
  private def updateAdapterMetrics(): F[Unit] = {
    for {
      databases <- getDatabases
      _ <- metricsAdapter.updateDatabaseMetrics(databases)
      _ <- metricsAdapter.updateTableMetrics(databases)
      _ <- metricsAdapter.updateSegmentMetrics(databases)
    } yield ()
  }

  override def getStats: F[CatalogStats] = {
    databaseManager.getStats
  }
  
  override def getDatabaseStats(dbName: String): F[Option[CatalogStats]] = {
    databaseManager.getDatabaseStats(dbName)
  }

  override def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]] = {
    databaseManager.getTableStats(dbName, tableName)
  }


}

object StatisticsService {

  def create[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:         MonitoringConfig = MonitoringConfig()
  ): F[StatisticsService[F]] = {
    createWithAdapter(databaseManager, config, MetricsAdapter.createNoOpAdapter(using summon[Async[F]]))
  }

  def createWithPrometheus[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:            MonitoringConfig = MonitoringConfig(),
    collectorRegistry: io.prometheus.client.CollectorRegistry
  ): F[StatisticsService[F]] = {
    val prometheusAdapter = MetricsAdapter.createPrometheusAdapter(collectorRegistry)
    for {
      service <- createWithAdapter(databaseManager, config, prometheusAdapter)
    } yield service
  }

  def createWithAdapter[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:         MonitoringConfig,
    metricsAdapter: MetricsAdapter[F]
  ): F[StatisticsService[F]] = {
    for {
      monitoringRef <- Ref.of[F, Boolean](false)
      service = new StatisticsServiceImpl(databaseManager, config, monitoringRef, metricsAdapter)
    } yield service
  }
}
