package org.nazaroid.kvdb.examples

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.Files
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.bitcask.storage.{StorageManager, Statistics}
import org.nazaroid.kvdb.statistics.{StatisticsIntegration, MonitoringConfig, MetricsAdapter}
import org.typelevel.log4cats.Logger

/**
 * Complete example of the entire statistics system
 * Shows how all components work together
 */
class CompleteStatisticsExample[F[_]: Async: Files: Logger](
  storageManager: StorageManager[F]
) {

  /** Example 1: Complete statistics workflow */
  def completeStatisticsWorkflow(): Resource[F, Unit] = {
    for {
      _ <- Logger[F].info("Starting complete statistics workflow")
      
      // 1. Create statistics integration with Prometheus adapter
      collectorRegistry <- Resource.make(
        Async[F].delay(new CollectorRegistry())
      )(registry => Async[F].delay(registry.clear()))
      
      statisticsIntegration <- Resource.eval(
        StatisticsIntegration.createWithPrometheus(storageManager, MonitoringConfig(), collectorRegistry)
      )
      
      // 2. Register metrics with Prometheus
      _ <- Resource.eval(
        statisticsIntegration.registerMetrics(collectorRegistry)
      )
      
      // 3. Start background monitoring
      _ <- Resource.make(
        statisticsIntegration.startMonitoring()
      )(_ => statisticsIntegration.stopMonitoring())
      
      // 4. Access statistics through different interfaces
      _ <- Resource.eval(
        // Direct storage manager access
        storageStats <- storageManager.getStats
        _ <- Logger[F].info(s"Direct storage stats: ${storageStats.totalTables} tables, ${storageStats.totalEntries} entries")
        
        // Through StatisticsIntegration
        integrationStats <- statisticsIntegration.getStats
        _ <- Logger[F].info(s"Integration stats: ${integrationStats.totalTables} tables, ${integrationStats.totalEntries} entries")
        
        // Through StatisticsService (if needed)
        // statisticsService <- StatisticsService.createWithPrometheus(storageManager, MonitoringConfig(), collectorRegistry)
        // serviceStats <- statisticsService.getStats
        // _ <- Logger[F].info(s"Service stats: ${serviceStats.totalTables} tables, ${serviceStats.totalEntries} entries")
      } yield ()
      
    } yield ()
  }

  /** Example 2: Statistics with different adapters */
  def demonstrateAdapterPattern(): F[Unit] = {
    for {
      _ <- Logger[F].info("Demonstrating adapter pattern")
      
      // Create different adapters
      prometheusAdapter <- Async[F].delay(
        MetricsAdapter.createPrometheusAdapter(new CollectorRegistry())
      )
      noOpAdapter <- Async[F].delay(
        MetricsAdapter.createNoOpAdapter()
      )
      
      // Create services with different adapters
      prometheusService <- Resource.eval(
        StatisticsService.createWithAdapter(storageManager, MonitoringConfig(), prometheusAdapter)
      )
      noOpService <- Resource.eval(
        StatisticsService.createWithAdapter(storageManager, MonitoringConfig(), noOpAdapter)
      )
      
      // Compare behavior
      prometheusStats <- prometheusService.getStats
      noOpStats <- noOpService.getStats
      
      _ <- Logger[F].info(s"Prometheus adapter stats: ${prometheusStats.totalEntries} entries")
      _ <- Logger[F].info(s"No-op adapter stats: ${noOpStats.totalEntries} entries")
      
      // Show that both delegate to storage manager
      _ <- if (prometheusStats.totalEntries == noOpStats.totalEntries) {
        Logger[F].info("✅ Both adapters delegate to storage manager correctly")
      } else {
        Logger[F].warn("❌ Adapter behavior differs")
      }
      
    } yield ()
  }

  /** Example 3: Real-time statistics monitoring */
  def realTimeMonitoring(): F[Unit] = {
    for {
      _ <- Logger[F].info("Starting real-time statistics monitoring")
      
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig(checkInterval = 5.seconds))
      )
      
      // Start monitoring
      _ <- integration.startMonitoring()
      
      // Monitor statistics changes in real-time
      monitoringStream = fs2.Stream.fixedRate[F](5.seconds)
        .evalMap(_ => integration.getStats)
        .evalMap { stats =>
          Logger[F].info(s"Real-time stats: ${stats.totalEntries} entries, ${stats.totalDataSize} bytes")
          
          // Log alerts for high fragmentation
          stats.tableStats.foreach { table =>
            if (table.entryCount > 1000) {
              Logger[F].warn(s"Table ${table.name} has high entry count: ${table.entryCount}")
            }
          }
          
          stats.segmentStats.foreach { segment =>
            if (segment.staleDataRatio > 0.5) {
              Logger[F].warn(s"Segment ${segment.name} has high fragmentation: ${segment.staleDataRatio}")
            }
          }
        }
      
      _ <- monitoringStream.compile.drain
      
    } yield ()
  }

  /** Example 4: Statistics export and analysis */
  def exportAndAnalyzeStatistics(): F[Unit] = {
    for {
      _ <- Logger[F].info("Exporting and analyzing statistics")
      
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      
      // Get current statistics
      stats <- integration.getStats
      
      // Export in different formats
      _ <- Logger[F].info("=== STATISTICS SUMMARY ===")
      _ <- Logger[F].info(s"Databases: ${stats.totalTables}")
      _ <- Logger[F].info(s"Total Entries: ${stats.totalEntries}")
      _ <- Logger[F].info(s"Active Entries: ${stats.activeEntries}")
      _ <- Logger[F].info(s"Deleted Entries: ${stats.deletedEntries}")
      _ <- Logger[F].info(s"Total Disk Size: ${stats.totalDataSize} bytes")
      
      // Table breakdown
      _ <- Logger[F].info("=== TABLE BREAKDOWN ===")
      _ <- stats.tableStats.traverse_ { table =>
        Logger[F].info(s"Table ${table.name}: ${table.entryCount} entries, ${table.activeEntryCount} active")
      }
      
      // Segment breakdown
      _ <- Logger[F].info("=== SEGMENT BREAKDOWN ===")
      _ <- stats.segmentStats.traverse_ { segment =>
        Logger[F].info(s"Segment ${segment.name}: ${segment.fileSize} bytes, active=${segment.isActive}, stale=${segment.staleDataRatio}")
      }
      
      // Export for Prometheus
      prometheusExport <- integration.exportForPrometheus()
      _ <- Logger[F].info("=== PROMETHEUS EXPORT ===")
      _ <- Logger[F].info(prometheusExport)
      
    } yield ()
  }
}

object CompleteStatisticsExample {
  
  /**
   * Run all statistics examples
   */
  def runAllExamples[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F]
  ): F[Unit] = {
    val example = new CompleteStatisticsExample(storageManager)
    
    for {
      _ <- example.completeStatisticsWorkflow()
      _ <- example.demonstrateAdapterPattern()
      _ <- example.realTimeMonitoring()
      _ <- example.exportAndAnalyzeStatistics()
    } yield ()
  }
  
  /**
   * Quick test of getStats functionality
   */
  def quickStatsTest[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F]
  ): F[Unit] = {
    for {
      _ <- Logger[F].info("Quick stats test")
      
      // Test all getStats methods
      storageStats <- storageManager.getStats
      _ <- Logger[F].info(s"StorageManager.getStats: ${storageStats.totalEntries} entries")
      
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      integrationStats <- integration.getStats
      _ <- Logger[F].info(s"StatisticsIntegration.getStats: ${integrationStats.totalEntries} entries")
      
      // Verify they're the same
      _ <- if (storageStats.totalEntries == integrationStats.totalEntries) {
        Logger[F].info("✅ All getStats methods work correctly")
      } else {
        Logger[F].error("❌ getStats methods return different results")
      }
      
    } yield ()
  }
}
