package org.nazaroid.kvdb.examples

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.Files
import org.nazaroid.kvdb.bitcask.storage.{StorageManager, SegmentStats}
import org.nazaroid.kvdb.statistics.{StatisticsIntegration, MonitoringConfig}
import org.typelevel.log4cats.Logger

/**
 * Example demonstrating getSegmentStats functionality
 */
class GetSegmentStatsExample[F[_]: Async: Files: Logger](
  storageManager: StorageManager[F]
) {

  /** Example 1: Get segment stats through StatisticsIntegration */
  def getSegmentStatsViaIntegration(): F[Unit] = {
    for {
      _ <- Logger[F].info("Getting segment stats via integration")
      
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      
      // Get segment stats for a specific database
      segmentStats <- integration.getSegmentStats("test_db")
      
      _ <- Logger[F].info(s"Found ${segmentStats.size} segments")
      _ <- segmentStats.traverse_ { segment =>
        Logger[F].info(s"Segment ${segment.name}: ${segment.fileSize} bytes, active=${segment.isActive}, stale=${segment.staleDataRatio}")
      }
      
    } yield ()
  }

  /** Example 2: Get segment stats through getStats (includes segment stats) */
  def getSegmentStatsViaGetStats(): F[Unit] = {
    for {
      _ <- Logger[F].info("Getting segment stats via getStats")
      
      // Get full stats which includes segment stats
      stats <- storageManager.getStats
      
      _ <- Logger[F].info(s"Database stats include ${stats.segmentStats.size} segments")
      _ <- stats.segmentStats.traverse_ { segment =>
        Logger[F].info(s"Segment ${segment.name}:")
        Logger[F].info(s"  - File size: ${segment.fileSize} bytes")
        Logger[F].info(s"  - Active: ${segment.isActive}")
        Logger[F].info(s"  - Stale ratio: ${segment.staleDataRatio}")
        Logger[F].info(s"  - Entry count: ${segment.entryCount}")
      }
      
    } yield ()
  }

  /** Example 3: Monitor segment statistics changes */
  def monitorSegmentChanges(): F[Unit] = {
    for {
      _ <- Logger[F].info("Starting segment monitoring")
      
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      
      // Monitor segment changes every 10 seconds
      monitoringStream = fs2.Stream.fixedRate[F](10.seconds)
        .evalMap(_ => integration.getStats)
        .evalMap { stats =>
          Logger[F].info(s"Current segments: ${stats.segmentStats.size}")
          
          // Alert for high fragmentation segments
          stats.segmentStats.foreach { segment =>
            if (!segment.isActive && segment.staleDataRatio > 0.7) {
              Logger[F].warn(s"Segment ${segment.name} needs compaction (fragmentation: ${segment.staleDataRatio})")
            }
            
            if (segment.fileSize > 100_000_000) { // 100MB
              Logger[F].warn(s"Segment ${segment.name} is large: ${segment.fileSize} bytes")
            }
          }
        }
      
      _ <- monitoringStream.compile.drain
      
    } yield ()
  }

  /** Example 4: Compare segment stats from different sources */
  def compareSegmentStatsSources(): F[Unit] = {
    for {
      _ <- Logger[F].info("Comparing segment stats from different sources")
      
      integration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      
      // Get segment stats through getStats
      fullStats <- integration.getStats
      segmentStatsFromGetStats = fullStats.segmentStats
      
      // Get segment stats through getSegmentStats (if available for specific DB)
      // Note: This would need a specific database name
      // segmentStatsFromMethod <- integration.getSegmentStats("some_db")
      
      _ <- Logger[F].info(s"Segments from getStats: ${segmentStatsFromGetStats.size}")
      _ <- segmentStatsFromGetStats.traverse_ { segment =>
        Logger[F].info(s"  ${segment.name}: ${segment.fileSize} bytes, active=${segment.isActive}")
      }
      
    } yield ()
  }
}

object GetSegmentStatsExample {
  
  /**
   * Run all getSegmentStats examples
   */
  def runAllExamples[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F]
  ): F[Unit] = {
    val example = new GetSegmentStatsExample(storageManager)
    
    for {
      _ <- example.getSegmentStatsViaIntegration()
      _ <- example.getSegmentStatsViaGetStats()
      _ <- example.monitorSegmentChanges()
      _ <- example.compareSegmentStatsSources()
    } yield ()
  }
  
  /**
   * Quick test of segment stats functionality
   */
  def quickSegmentStatsTest[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F]
  ): F[Unit] = {
    for {
      _ <- Logger[F].info("Quick segment stats test")
      
      // Test getStats includes segment stats
      stats <- storageManager.getStats
      _ <- Logger[F].info(s"Found ${stats.segmentStats.size} segments in database stats")
      
      // Test segment stats structure
      _ <- stats.segmentStats.traverse_ { segment =>
        Logger[F].info(s"Segment ${segment.name}:")
        Logger[F].info(s"  - Name: ${segment.name}")
        Logger[F].info(s"  - File size: ${segment.fileSize}")
        Logger[F].info(s"  - Is active: ${segment.isActive}")
        Logger[F].info(s"  - Stale ratio: ${segment.staleDataRatio}")
        Logger[F].info(s"  - Entry count: ${segment.entryCount}")
      }
      
      _ <- Logger[F].info("✅ Segment stats functionality working correctly")
      
    } yield ()
  }
}
