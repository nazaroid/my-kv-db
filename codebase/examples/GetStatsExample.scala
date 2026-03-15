package org.nazaroid.kvdb.examples

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.Files
import org.nazaroid.kvdb.bitcask.storage.{StorageManager, Statistics}
import org.nazaroid.kvdb.statistics.{StatisticsIntegration, MonitoringConfig}
import org.typelevel.log4cats.Logger

/**
 * Example of using getStats method to access storage manager statistics
 */
class GetStatsExample[F[_]: Async: Files: Logger](
  storageManager: StorageManager[F]
) {

  /** Example 1: Direct usage of getStats */
  def getStorageStatsDirectly(): F[Unit] = {
    for {
      _ <- Logger[F].info("Getting storage statistics directly")
      
      // Call getStats on storage manager
      stats <- storageManager.getStats
      
      _ <- Logger[F].info(s"Storage stats: ${stats.totalTables} tables, ${stats.totalEntries} entries")
      _ <- Logger[F].info(s"Active entries: ${stats.activeEntries}, Deleted entries: ${stats.deletedEntries}")
      _ <- Logger[F].info(s"Total disk size: ${stats.totalDataSize} bytes")
      
      // Log table statistics
      _ <- stats.tableStats.traverse_ { table =>
        Logger[F].info(s"Table ${table.name}: ${table.entryCount} entries, ${table.activeEntryCount} active")
      }
      
      // Log segment statistics  
      _ <- stats.segmentStats.traverse_ { segment =>
        Logger[F].info(s"Segment ${segment.name}: ${segment.fileSize} bytes, active=${segment.isActive}, stale ratio=${segment.staleDataRatio}")
      }
      
    } yield ()
  }

  /** Example 2: Using getStats through StatisticsIntegration */
  def getStorageStatsViaIntegration(): F[Unit] = {
    for {
      _ <- Logger[F].info("Getting storage statistics via integration")
      
      // Create statistics integration
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      
      // Use getStats method
      stats <- integration.getStats
      
      _ <- Logger[F].info(s"Integration stats: ${stats.totalTables} tables, ${stats.totalEntries} entries")
      _ <- Logger[F].info(s"Active entries: ${stats.activeEntries}, Deleted entries: ${stats.deletedEntries}")
      _ <- Logger[F].info(s"Total disk size: ${stats.totalDataSize} bytes")
      
    } yield ()
  }

  /** Example 3: Comparing different statistics sources */
  def compareStatisticsSources(): F[Unit] = {
    for {
      _ <- Logger[F].info("Comparing statistics sources")
      
      // Get stats from storage manager directly
      storageStats <- storageManager.getStats
      
      // Get stats through integration
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      integrationStats <- integration.getStats
      
      // Compare results
      _ <- Logger[F].info(s"Storage tables: ${storageStats.totalTables}")
      _ <- Logger[F].info(s"Integration tables: ${integrationStats.totalTables}")
      
      _ <- Logger[F].info(s"Storage entries: ${storageStats.totalEntries}")
      _ <- Logger[F].info(s"Integration entries: ${integrationStats.totalEntries}")
      
      _ <- Logger[F].info(s"Storage active: ${storageStats.activeEntries}")
      _ <- Logger[F].info(s"Integration active: ${integrationStats.activeEntries}")
      
      // Should be the same since integration delegates to storage manager
      _ <- if (storageStats.totalTables == integrationStats.totalTables) {
        Logger[F].info("✅ Table counts match")
      } else {
        Logger[F].warn("❌ Table counts differ")
      }
      
      _ <- if (StorageStats.totalEntries == integrationStats.totalEntries) {
        Logger[F].info("✅ Entry counts match")
      } else {
        Logger[F].warn("❌ Entry counts differ")
      }
      
    } yield ()
  }

  /** Example 4: Monitoring statistics changes over time */
  def monitorStatisticsChanges(): F[Unit] = {
    for {
      _ <- Logger[F].info("Starting statistics monitoring")
      
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      
      // Monitor changes every 10 seconds
      monitoringStream = fs2.Stream.fixedRate[F](10.seconds)
        .evalMap(_ => integration.getStats)
        .evalMap { currentStats =>
          Logger[F].info(s"Current stats: ${currentStats.totalEntries} entries, ${currentStats.totalDataSize} bytes")
        }
        .handleErrorWith { error =>
          Logger[F].error(s"Error monitoring stats: $error")
          // Continue monitoring
          fs2.Stream.eval(Async[F].unit)
        }
      
      _ <- monitoringStream.compile.drain
      
    } yield ()
  }

  /** Example 5: Exporting statistics in different formats */
  def exportStatisticsInFormats(): F[Unit] = {
    for {
      _ <- Logger[F].info("Exporting statistics in different formats")
      
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      
      stats <- integration.getStats
      
      // Export as JSON
      _ <- Logger[F].info(s"JSON export: ${stats}")
      
      // Export via Prometheus format (legacy method)
      prometheusExport <- integration.exportForPrometheus()
      _ <- Logger[F].info("Prometheus export:")
      _ <- Logger[F].info(prometheusExport)
      
    } yield ()
  }
}

object GetStatsExample {
  
  /**
   * Complete example of getStats usage
   */
  def runAllExamples[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F]
  ): F[Unit] = {
    val example = new GetStatsExample(storageManager)
    
    for {
      _ <- example.getStorageStatsDirectly()
      _ <- example.getStorageStatsViaIntegration()
      _ <- example.compareStatisticsSources()
      _ <- example.monitorStatisticsChanges()
      _ <- example.exportStatisticsInFormats()
    } yield ()
  }
  
  /**
   * Simple getStats usage
   */
  def simpleGetStats[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F]
  ): F[Statistics] = {
    for {
      _ <- Logger[F].info("Getting storage statistics")
      stats <- storageManager.getStats
      _ <- Logger[F].info(s"Found ${stats.totalTables} tables with ${stats.totalEntries} total entries")
    } yield stats
  }
}
