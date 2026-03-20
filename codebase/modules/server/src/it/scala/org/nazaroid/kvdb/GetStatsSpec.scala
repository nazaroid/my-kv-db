package org.nazaroid.kvdb

import cats.effect.{Async, IO}
import cats.effect.testing.ResourceSpec
import cats.implicits.given
import org.nazaroid.kvdb.algebra.DatabaseStats
import org.nazaroid.kvdb.srv.BitcaskEngineConfig
import org.nazaroid.kvdb.statistics.{MonitoringConfig, StatisticsIntegration}
import org.typelevel.log4cats.slf4j.Slf4jLogger

/**
 * Simple test for getStats functionality
 */
class GetStatsSpec extends ResourceSpec[IO] {

  override def isParallel: Boolean = false

  "getStats method" should {
    
    "return database statistics through engine" in {
      withTestEngine { engine =>
        for {
          // Test getStats through engine
          stats <- engine.getStats
          
          _ = stats should not be(null)
          _ = stats.totalTables should be >= 0
          _ = stats.totalEntries should be >= 0
          _ = stats.activeEntries should be >= 0
          _ = stats.deletedEntries should be >= 0
          _ = stats.totalDataSize should be >= 0
          _ = stats.tableStats should not be(empty)
          _ = stats.segmentStats should not be(empty)
        } yield ()
      }
    }
    
    "return consistent results across different access methods" in {
      withTestEngine { engine =>
        for {
          // Get stats through engine
          engineStats <- engine.getStats
          
          // Get stats through integration
          integration <- StatisticsIntegration.create(engine.catalog.storageManager, MonitoringConfig())
          integrationStats <- integration.getStats
          
          // Should be the same
          _ = engineStats.totalTables should be(integrationStats.totalTables)
          _ = engineStats.totalEntries should be(integrationStats.totalEntries)
          _ = engineStats.activeEntries should be(integrationStats.activeEntries)
          _ = engineStats.deletedEntries should be(integrationStats.deletedEntries)
          _ = engineStats.totalDataSize should be(integrationStats.totalDataSize)
        } yield ()
      }
    }
    
    "reflect data changes" in {
      withTestEngine { engine =>
        for {
          // Get initial stats
          initialStats <- engine.getStats
          
          // Add some data
          _ <- engine.set("test_db", "test_table", "key1", "value1")
          _ <- engine.set("test_db", "test_table", "key2", "value2")
          
          // Get updated stats
          updatedStats <- engine.getStats
          
          // Should reflect the changes
          _ = updatedStats.totalEntries should be > initialStats.totalEntries
          _ = updatedStats.activeEntries should be > initialStats.activeEntries
          
          // Clean up
          _ <- engine.delete("test_db", "test_table", "key1")
          _ <- engine.delete("test_db", "test_table", "key2")
          
          // Get final stats
          finalStats <- engine.getStats
          
          // Should reflect deletions
          _ = finalStats.deletedEntries should be > initialStats.deletedEntries
        } yield ()
      }
    }
    
    "handle empty database gracefully" in {
      withTestEngine { engine =>
        for {
          // Get stats from empty database
          stats <- engine.getStats
          
          _ = stats.totalTables should be(0)
          _ = stats.totalEntries should be(0)
          _ = stats.activeEntries should be(0)
          _ = stats.deletedEntries should be(0)
          _ = stats.totalDataSize should be(0)
          _ = stats.tableStats should be(empty)
          _ = stats.segmentStats should not be(empty) // Should have at least one segment
        } yield ()
      }
    }
  }
  
  private def withTestEngine[A](test: org.nazaroid.kvdb.algebra.Engine[IO] => IO[A]): IO[A] = {
    for {
      given Logger[IO] <- IO(Slf4jLogger.create[IO])
      
      // Create test engine
      engine <- org.nazaroid.kvdb.engine.BitcaskEngine.init[IO](BitcaskEngineConfig(
        rootDir = "/tmp/test-kvdb-getstats",
        maxSegmentSize = 1024 * 1024,
        maxSegmentCount = 10,
        fileWriteBufferSize = 1024,
        fileWriteParallelism = 2
      ))
      
      result <- test(engine)
      
    } yield result
  }
}
