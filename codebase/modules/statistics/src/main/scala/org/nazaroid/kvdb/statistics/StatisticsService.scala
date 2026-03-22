package org.nazaroid.kvdb.statistics

import cats.effect.implicits.given
import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.io.file.{Files, Path}
import io.circe.*
import org.nazaroid.kvdb.core.{CatalogStats, DatabaseInfo, DatabaseManager, DatabaseStats, SegmentInfo, TableInfo}
import org.nazaroid.kvdb.bitcask.metrics.{BitcaskPrometheusMetricsAdapter, MetricsAdapter}
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
  databaseManager: DatabaseManager[F],
  config:         MonitoringConfig,
  monitoringRef:  Ref[F, Boolean],
  metricsAdapter: MetricsAdapter[F] // Injected via constructor
) extends StatisticsService[F] {

  override def registerMetrics(): F[Unit] = {
    for {
      _ <- Logger[F].info("Registering metrics with adapter")
      _ <- metricsAdapter.registerMetrics()
      // Initial update with current values
      stats <- getStats
      _ <- metricsAdapter.updateMetrics(stats)
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
      stats <- getStats
      _ <- metricsAdapter.updateMetrics(stats)
    } yield ()
  }

  override def getStats: F[CatalogStats] = {
    databaseManager.getStats
  }
  
  override def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] = {
    databaseManager.getDatabaseStats(dbName)
  }

  override def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]] = {
    databaseManager.getTableStats(dbName, tableName)
  }
  
  override def getDatabases: F[List[DatabaseInfo]] = {
    for {
      dbNames <- databaseManager.listDatabases
      dbInfos   <- dbNames.traverse(databaseManager.getDatabaseStats)
    } yield dbInfos.flatten
  }


}

object StatisticsService {

  def create[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:         MonitoringConfig = MonitoringConfig()
  ): F[StatisticsService[F]] = {
    createWithAdapter(databaseManager, config, BitcaskPrometheusMetricsAdapter.create)
  }

  def createWithPrometheus[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:         MonitoringConfig = MonitoringConfig()
  ): F[StatisticsService[F]] = {
    createWithAdapter(databaseManager, config, BitcaskPrometheusMetricsAdapter.create)
  }

  def createWithAdapter[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:         MonitoringConfig,
    adapterFactory: F[MetricsAdapter[F]]
  ): F[StatisticsService[F]] = {
    for {
      metricsAdapter <- adapterFactory
      monitoringRef  <- Ref.of[F, Boolean](false)
    } yield new StatisticsServiceImpl(
      databaseManager = databaseManager,
      config = config,
      monitoringRef = monitoringRef,
      metricsAdapter = metricsAdapter
    )
  }
}
