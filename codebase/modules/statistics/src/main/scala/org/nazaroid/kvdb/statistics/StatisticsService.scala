package org.nazaroid.kvdb.statistics

import cats.effect.implicits.given
import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.io.file.{Files, Path}
import io.circe.*
import org.nazaroid.kvdb.core.{DatabaseManager, DatabaseInfo, TableInfo, SegmentInfo, DatabaseStats}
import org.typelevel.log4cats.Logger

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Background process for monitoring segments and fragmentation */
trait StatisticsService[F[_]] {
  def startMonitoring():                F[Unit]
  def stopMonitoring():                 F[Unit]
  def getDatabases:                     F[List[DatabaseInfo]]
  def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]]
  def getSegmentStats(dbName: String):  F[List[SegmentInfo]]
  def getStats:                         F[DatabaseStats]  // Delegate to databaseManager
  def registerMetrics():                F[Unit]
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

  override def getStats: F[DatabaseStats] = {
    databaseManager.getStats
  }

  override def startMonitoring(): F[Unit] = {
    if (config.enableBackgroundMonitoring) {
      // Define the monitoring stream recursively to ensure it restarts after errors
      lazy val monitoringStream: Stream[F, Unit] = Stream
        .fixedRate[F](config.checkInterval)
        .evalMap(_ => collectStatistics())
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

  override def getDatabases: F[List[DatabaseInfo]] = {
    for {
      dbNames <- databaseManager.listDatabases
      dbInfos   <- dbNames.traverse(collectDatabaseInfo)
    } yield dbInfos.flatten
  }

  override def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] = {
    for {
      db <- databaseManager.getDatabase(dbName)
      dbInfo <- db.traverse(collectDatabaseInfo)
    } yield dbInfo.flatten.headOption
  }

  override def getSegmentStats(dbName: String): F[List[SegmentInfo]] = {
    for {
      dbPath   <- getDatabasePath(dbName)
      segments <- collectSegmentInfo(dbPath)
    } yield segments
  }

  /** Collect statistics for all databases */
  private def collectStatistics(): F[Unit] = {
    for {
      databases <- getDatabases
      _ <- databases.traverse_ { db =>
        for {
          _ <-
            if (db.fragmentationRatio > config.maxStaleRatio) {
              Logger[F].warn(s"Database ${db.name} has high fragmentation: ${db.fragmentationRatio}")
            } else ().pure[F]

          segments <- getSegmentStats(db.name)
          _ <- segments.traverse_ { segment =>
            if (!segment.isActive && segment.staleDataRatio > config.compactionThreshold) {
              Logger[F].info(s"Segment ${segment.name} in database ${db.name} needs compaction")
            } else ().pure[F]
          }
        } yield ()
      }
      _ <- updateAdapterMetrics()
    } yield ()
  }

  /** Get all database folders */
  private def getAllDatabaseFolders: F[List[Path]] = {
    // Assuming databases are in subdirectories of the main folder
    Files[F]
      .list(Path(storageManager.config.folder))
      .filter(_.fileName.toString.endsWith(".db"))
      .compile
      .toList
  }

  /** Get path for specific database */
  private def getDatabasePath(dbName: String): F[Path] = {
    Async[F].pure(Path(storageManager.config.folder) / s"$dbName.db")
  }

  /** Collect database information from disk and memory */
  private def collectDatabaseInfo(dbPath: Path): F[Option[DatabaseInfo]] = {
    val dbName = dbPath.fileName.toString.replace(".db", "")

    Files[F].exists(dbPath).flatMap {
      case false =>
        Async[F].pure(None)
      case true =>
        for {
          storageStats <- storageManager.getStats
          segments     <- collectSegmentInfo(dbPath)
          tables       <- collectTableInfo(dbPath, segments)

          // Вспомогательные расчеты
          totalDiskSize = segments.map(_.fileSize).sum
          totalEntries = segments.map(_.entryCount).sum
          activeEntries = segments.filter(_.isActive).map(_.entryCount).sum

          // Исправлен расчет fragmentationRatio (избегаем дублирования суммы)
          fragmentationRatio =
            if (totalDiskSize > 0) {
              segments.map(s => s.staleDataRatio * s.fileSize).sum / totalDiskSize
            } else 0.0

          dbInfo = DatabaseInfo(
            name               = dbName,
            tables             = tables,
            totalEntries       = totalEntries,
            activeEntries      = activeEntries,
            deletedEntries     = totalEntries - activeEntries,
            totalDiskSize      = totalDiskSize,
            totalMemorySize    = storageStats.totalDataSize,
            fragmentationRatio = fragmentationRatio
          )
        } yield Some(dbInfo)
    }
  }

  /** Collect segment information from disk files */
  private def collectSegmentInfo(dbPath: Path): F[List[SegmentInfo]] = {
    for {
      segmentFiles <- Files[F]
        .list(dbPath)
        .filter(_.fileName.toString.endsWith(".bin"))
        .filter(_.fileName.toString.startsWith("seg_"))
        .compile
        .toList

      // Get active segments from storage manager
      storageStats <- storageManager.getStats
      activeSegmentNames = storageStats.segmentStats.filter(_.isActive).map(_.name).toSet

      segmentInfos <- segmentFiles.traverse { segmentFile =>
        val segmentName = segmentFile.fileName.toString.replace(".bin", "")

        for {
          fileSize     <- Files[F].size(segmentFile)
          lastModified <- Files[F].getLastModifiedTime(segmentFile).map(_.toUnit(TimeUnit.MILLISECONDS).toLong)

          // Calculate stale data ratio by analyzing segment content
          staleRatio <- calculateStaleDataRatio(segmentFile)

          // Count entries (simplified - would need actual parsing)
          entryCount <- countSegmentEntries(segmentFile)

        } yield SegmentInfo(
          name           = segmentName,
          filePath       = segmentFile.toString,
          fileSize       = fileSize,
          isActive       = activeSegmentNames.contains(segmentName),
          staleDataRatio = staleRatio,
          entryCount     = entryCount,
          lastModified   = lastModified
        )
      }

    } yield segmentInfos
  }

  /** Collect table information from segment files */
  private def collectTableInfo(dbPath: Path, segments: List[SegmentInfo]): F[List[TableInfo]] = {
    // Extract table names from keys in segments (simplified approach)
    val tableNames = segments.flatMap { segment =>
      // In a real implementation, this would parse segment files to extract table names
      // For now, we'll use a simplified approach
      List("default_table") // Placeholder
    }.distinct

    tableNames
      .map { tableName =>
        // Calculate table statistics from segments
        val tableSegments = segments.filter(_.name.contains(tableName))
        val entryCount = tableSegments.map(_.entryCount).sum
        val activeEntryCount = tableSegments.filter(_.isActive).map(_.entryCount).sum
        val diskSize = tableSegments.map(_.fileSize).sum
        val memorySize = entryCount * 100L // Estimate (100 bytes per entry)

        TableInfo(
          name             = tableName,
          entryCount       = entryCount,
          activeEntryCount = activeEntryCount,
          diskSize         = diskSize,
          memorySize       = memorySize
        )
      }
      .pure[F]
  }

  /** Calculate stale data ratio for a segment */
  private def calculateStaleDataRatio(segmentFile: Path): F[Double] = {
    // This is a simplified implementation
    // In reality, this would parse the segment file and count deleted/updated entries
    for {
      fileSize <- Files[F].size(segmentFile)
      // Assume 20% stale data as a placeholder
      staleRatio = 0.2
    } yield staleRatio
  }

  /** Count entries in a segment file */
  private def countSegmentEntries(segmentFile: Path): F[Int] = {
    // Simplified implementation - would need to parse binary format
    for {
      fileSize <- Files[F].size(segmentFile)
      // Assume average entry size of 100 bytes
      entryCount = (fileSize / 100).toInt
    } yield entryCount
  }

  /** Update metrics through adapter */
  private def updateAdapterMetrics(): F[Unit] = {
    for {
      databases <- getDatabases
      _         <- metricsAdapter.updateDatabaseMetrics(databases)
      _         <- metricsAdapter.updateTableMetrics(databases)
      _         <- metricsAdapter.updateSegmentMetrics(databases)
    } yield ()
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
