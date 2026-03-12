package org.nazaroid.kvdb.statistics

import cats.effect.{Async, Ref, Resource}
import cats.effect.kernel.Outcome
import cats.implicits.given
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.bitcask.storage.{StorageManager, Statistics}
import org.typelevel.log4cats.Logger
import scala.concurrent.duration.*

import java.nio.file.{Files => JFiles}
import java.util.concurrent.TimeUnit

/** Background process for monitoring segments and fragmentation */
trait StatisticsService[F[_]] {
  def startMonitoring(): F[Unit]
  def stopMonitoring(): F[Unit]
  def getDatabases: F[List[DatabaseInfo]]
  def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]]
  def getSegmentStats(dbName: String): F[List[SegmentInfo]]
  def setMetricsAdapter(adapter: MetricsAdapter[F]): F[Unit]
  def registerMetrics(): F[Unit]
}

case class DatabaseInfo(
  name: String,
  tables: List[TableInfo],
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDiskSize: Long,
  totalMemorySize: Long,
  fragmentationRatio: Double
)

case class TableInfo(
  name: String,
  entryCount: Int,
  activeEntryCount: Int,
  diskSize: Long,
  memorySize: Long
)

case class SegmentInfo(
  name: String,
  filePath: String,
  fileSize: Long,
  isActive: Boolean,
  staleDataRatio: Double,
  entryCount: Int,
  lastModified: Long
)

case class MonitoringConfig(
  checkInterval: FiniteDuration = 30.seconds,
  enableBackgroundMonitoring: Boolean = true,
  maxStaleRatio: Double = 0.3,
  compactionThreshold: Double = 0.5
)

class StatisticsServiceImpl[F[_]: Async: Files: Logger](
  storageManager: StorageManager[F],
  config: MonitoringConfig,
  monitoringRef: Ref[F, Boolean]
) extends StatisticsService[F] {

  // Metrics adapter for exporting to different collectors
  private var metricsAdapter: Option[MetricsAdapter[F]] = None
  
  override def setMetricsAdapter(adapter: MetricsAdapter[F]): F[Unit] = {
    for {
      _ <- Logger[F].info("Setting metrics adapter")
      _ <- Async[F].delay {
        metricsAdapter = Some(adapter)
      }
    } yield ()
  }
  
  override def registerMetrics(): F[Unit] = {
    metricsAdapter.traverse_ { adapter =>
      for {
        _ <- Logger[F].info("Registering metrics with adapter")
        _ <- adapter.registerDatabaseMetrics()
        _ <- adapter.registerTableMetrics()
        _ <- adapter.registerSegmentMetrics()
        // Initial update with current values
        _ <- updateAdapterMetrics()
      } yield ()
    }
  }

  override def startMonitoring(): F[Unit] = {
    if (config.enableBackgroundMonitoring) {
      Logger[F].info("Starting statistics monitoring service")
      
      val monitoringStream = Stream
        .fixedRate[F](config.checkInterval)
        .evalMap(_ => collectStatistics())
        .handleErrorWith(error => 
          Logger[F].error(s"Error in monitoring stream: $error") *> 
          Async[F].sleep(5.seconds) // Back off on error
        )
      
      monitoringRef.set(true) *>
      monitoringStream.compile.drain.attempt.void
    } else {
      Logger[F].info("Background monitoring disabled")
      Async[F].unit
    }
  }

  override def stopMonitoring(): F[Unit] = {
    Logger[F].info("Stopping statistics monitoring service")
    monitoringRef.set(false)
  }

  override def getDatabases: F[List[DatabaseInfo]] = {
    for {
      dbFolders <- getAllDatabaseFolders()
      dbInfos <- dbFolders.traverse(collectDatabaseInfo)
    } yield dbInfos.flatten
  }

  override def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] = {
    getDatabases.map(_.find(_.name == dbName))
  }

  override def getSegmentStats(dbName: String): F[List[SegmentInfo]] = {
    for {
      dbPath <- getDatabasePath(dbName)
      segments <- collectSegmentInfo(dbPath)
    } yield segments
  }
  
  /** Collect statistics for all databases */
  private def collectStatistics(): F[Unit] = {
    for {
      databases <- getDatabases
      _ <- databases.traverse_ { db =>
        if (db.fragmentationRatio > config.maxStaleRatio) {
          Logger[F].warn(s"Database ${db.name} has high fragmentation: ${db.fragmentationRatio}")
        }
        
        // Check segments that need compaction
        segments <- getSegmentStats(db.name)
        _ <- segments.traverse_ { segment =>
          if (!segment.isActive && segment.staleDataRatio > config.compactionThreshold) {
            Logger[F].info(s"Segment ${segment.name} in database ${db.name} needs compaction (stale ratio: ${segment.staleDataRatio})")
          }
        }
      }
      
      // Update metrics through adapter
      _ <- updateAdapterMetrics()
    } yield ()
  }

  /** Get all database folders */
  private def getAllDatabaseFolders(): F[List[Path]] = {
    // Assuming databases are in subdirectories of the main folder
    Files[F].list(Path(storageManager.config.folder))
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
    
    for {
      exists <- Files[F].exists(dbPath)
      _ <- if (!exists) Async[F].pure(None) else Async[F].unit
      
      // Get storage manager stats for active data
      storageStats <- storageManager.getStats
      
      // Collect segment information from disk
      segments <- collectSegmentInfo(dbPath)
      
      // Collect table information
      tables <- collectTableInfo(dbPath, segments)
      
      // Calculate totals
      totalEntries = segments.map(_.entryCount).sum
      activeEntries = segments.filter(_.isActive).map(_.entryCount).sum
      deletedEntries = totalEntries - activeEntries
      totalDiskSize = segments.map(_.fileSize).sum
      totalMemorySize = storageStats.totalDataSize
      fragmentationRatio = if (totalDiskSize > 0) {
        segments.map(_.staleDataRatio * segments.map(_.fileSize).sum).sum / totalDiskSize
      } else 0.0
      
      dbInfo = DatabaseInfo(
        name = dbName,
        tables = tables,
        totalEntries = totalEntries,
        activeEntries = activeEntries,
        deletedEntries = deletedEntries,
        totalDiskSize = totalDiskSize,
        totalMemorySize = totalMemorySize,
        fragmentationRatio = fragmentationRatio
      )
      
    } yield Some(dbInfo)
  }

  /** Collect segment information from disk files */
  private def collectSegmentInfo(dbPath: Path): F[List[SegmentInfo]] = {
    for {
      segmentFiles <- Files[F].list(dbPath)
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
          fileSize <- Files[F].size(segmentFile)
          lastModified <- Files[F].getLastModifiedTime(segmentFile).map(_.to(TimeUnit.MILLISECONDS))
          
          // Calculate stale data ratio by analyzing segment content
          staleRatio <- calculateStaleDataRatio(segmentFile)
          
          // Count entries (simplified - would need actual parsing)
          entryCount <- countSegmentEntries(segmentFile)
          
        } yield SegmentInfo(
          name = segmentName,
          filePath = segmentFile.toString,
          fileSize = fileSize,
          isActive = activeSegmentNames.contains(segmentName),
          staleDataRatio = staleRatio,
          entryCount = entryCount,
          lastModified = lastModified
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
    
    tableNames.traverse { tableName =>
      for {
        // Calculate table statistics from segments
        tableSegments = segments.filter(_.name.contains(tableName))
        entryCount = tableSegments.map(_.entryCount).sum
        activeEntryCount = tableSegments.filter(_.isActive).map(_.entryCount).sum
        diskSize = tableSegments.map(_.fileSize).sum
        memorySize = entryCount * 100L // Estimate (100 bytes per entry)
        
      } yield TableInfo(
        name = tableName,
        entryCount = entryCount,
        activeEntryCount = activeEntryCount,
        diskSize = diskSize,
        memorySize = memorySize
      )
    }
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
    metricsAdapter.traverse_ { adapter =>
      for {
        databases <- getDatabases
        _ <- adapter.updateDatabaseMetrics(databases)
        _ <- adapter.updateTableMetrics(databases)
        _ <- adapter.updateSegmentMetrics(databases)
      } yield ()
    }
  }
}

object StatisticsService {
  def create[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F],
    config: MonitoringConfig = MonitoringConfig()
  ): F[StatisticsService[F]] = {
    for {
      monitoringRef <- Ref.of[F, Boolean](false)
      service = new StatisticsServiceImpl(storageManager, config, monitoringRef)
    } yield service
  }
}
