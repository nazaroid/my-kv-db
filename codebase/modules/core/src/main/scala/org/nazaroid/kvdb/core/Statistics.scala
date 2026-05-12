package org.nazaroid.kvdb.core

import cats.effect.implicits.given
import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.io.file.Files
import io.circe.Json
import org.nazaroid.kvdb.core.*
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.*

/** Database statistics for multiple databases
  */
case class CatalogStats(
  totalDatabases: Int,
  totalTables:    Int,
  totalEntries:   Int,
  activeEntries:  Int,
  deletedEntries: Int,
  totalDataSize:  Long,
  // Heterogeneous collection for engine-specific details
  details: Map[String, Json] = Map.empty)

/** Database information for single database
  */
case class DatabaseInfo(
  name:           String,
  totalTables:    Int,
  totalEntries:   Int,
  activeEntries:  Int,
  deletedEntries: Int,
  totalDataSize:  Long,
  // Engine-specific details
  details: Map[String, Json] = Map.empty)

/** Table information
  */
case class TableInfo(
  name:              String,
  entryCount:        Int,
  activeEntryCount:  Int,
  deletedEntryCount: Int,
  totalDataSize:     Long,
  // Engine-specific details
  details: Map[String, Json] = Map.empty)

/** Segment information (for storage-like engines)
  */
case class SegmentInfo(
  name:       String,
  fileSize:   Long,
  isActive:   Boolean,
  entryCount: Int,
  // Engine-specific details
  details: Map[String, Json] = Map.empty)

/** Background process for monitoring segments and fragmentation */
trait StatisticsService[F[_]] {
  def startMonitoring(): F[Unit]
  def stopMonitoring():  F[Unit]
  def getDatabases:      F[List[DatabaseInfo]]

  def getStats:                                         F[CatalogStats]
  def getDatabaseStats(dbName: String):                 F[Option[DatabaseInfo]]
  def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]]
}

case class MonitoringConfig(
  checkInterval:              FiniteDuration = 30.seconds,
  enableBackgroundMonitoring: Boolean = true,
  maxStaleRatio:              Double = 0.3,
  compactionThreshold:        Double = 0.5)

class StatisticsServiceImpl[F[_]: Async: Files: Logger](
  databaseManager: DatabaseManager[F],
  config:          MonitoringConfig,
  monitoringRef:   Ref[F, Boolean],
  metricsAdapter:  CatalogMetricRecorder[F])
    extends StatisticsService[F] {

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
      _     <- metricsAdapter.recordMetrics(stats)
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
      dbInfos <- dbNames.traverse(databaseManager.getDatabaseStats)
    } yield dbInfos.flatten
  }

}

object StatisticsService {

  def createWithAdapter[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:          MonitoringConfig,
    adapter:         F[CatalogMetricRecorder[F]]
  ): F[StatisticsService[F]] = {
    for {
      metricsAdapter <- adapter
      monitoringRef  <- Ref.of[F, Boolean](false)
    } yield new StatisticsServiceImpl(
      databaseManager = databaseManager,
      config = config,
      monitoringRef = monitoringRef,
      metricsAdapter = metricsAdapter
    )
  }
}
