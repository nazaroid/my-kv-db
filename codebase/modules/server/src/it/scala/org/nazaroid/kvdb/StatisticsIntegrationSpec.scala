package org.nazaroid.kvdb

import cats.effect.{Async, IO}
import cats.effect.testing.ResourceSpec
import cats.implicits.given
import io.circe.syntax.*
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.client.Client
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.Uri
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.nazaroid.kvdb.algebra.{DatabaseInfo, DatabaseStats, SegmentInfo}
import org.nazaroid.kvdb.srv.{DbInstanceConfig, EngineConfig, ServerConfig}
import org.nazaroid.kvdb.statistics.{MonitoringConfig, StatisticsIntegration}

/**
 * Integration tests for statistics endpoints
 */
class StatisticsIntegrationSpec extends ResourceSpec[IO] {

  override def isParallel: Boolean = false

  "Statistics endpoints" should {
    
    "return storage statistics" in {
      withTestServer { (client, server) =>
        for {
          // Test storage stats endpoint
          response <- client.expect[DatabaseStats](GET / "stats")
          
          _ = response should not be(null)
          _ = response.totalTables should be >= 0
          _ = response.totalEntries should be >= 0
          _ = response.activeEntries should be >= 0
          _ = response.deletedEntries should be >= 0
          _ = response.totalDataSize should be >= 0
          _ = response.tableStats should not be(empty)
          _ = response.segmentStats should not be(empty)
        } yield ()
    }
    
    "return all databases" in {
      withTestServer { (client, server) =>
        for {
          // Test databases endpoint
          response <- client.expect[List[DatabaseInfo]](GET / "databases")
          
          _ = response should not be(null)
          _ = response should be(a[List[DatabaseInfo]])
        } yield ()
      }
    }
    
    "return specific database stats" in {
      withTestServer { (client, server) =>
        for {
          // Create a test database first
          _ <- client.expect[String](POST / "test_db")
          
          // Test database stats endpoint
          response <- client.expect[DatabaseInfo](GET / "database" / "test_db")
          
          _ = response should not be(null)
          _ = response.name should be("test_db")
          _ = response.totalEntries should be >= 0
          _ = response.activeEntries should be >= 0
          _ = response.deletedEntries should be >= 0
        } yield ()
      }
    }
    
    "return segment statistics" in {
      withTestServer { (client, server) =>
        for {
          // Create a test database first
          _ <- client.expect[String](POST / "test_db")
          
          // Test segment stats endpoint
          response <- client.expect[List[SegmentInfo]](GET / "segments" / "test_db")
          
          _ = response should not be(null)
          _ = response should be(a[List[SegmentInfo]])
        } yield ()
      }
    }
    
    "return Prometheus export" in {
      withTestServer { (client, server) =>
        for {
          // Test Prometheus endpoint
          response <- client.expect[String](GET / "prometheus")
          
          _ = response should not be(null)
          _ = response should include("# HELP")
          _ = response should include("# TYPE")
          _ = response should include("kvdb_")
        } yield ()
      }
    }
    
    "start and stop monitoring" in {
      withTestServer { (client, server) =>
        for {
          // Test start monitoring
          startResponse <- client.expect[String](POST / "monitoring" / "start")
          _ = startResponse should be("Monitoring started")
          
          // Test stop monitoring
          stopResponse <- client.expect[String](POST / "monitoring" / "stop")
          _ = stopResponse should be("Monitoring stopped")
        } yield ()
      }
    }
    
    "handle monitoring when already started" in {
      withTestServer { (client, server) =>
        for {
          // Start monitoring twice
          _ <- client.expect[String](POST / "monitoring" / "start")
          secondStartResponse <- client.expect[String](POST / "monitoring" / "start")
          _ = secondStartResponse should include("already") or include("running")
        } yield ()
      }
    }
    
    "return 404 for non-existent database" in {
      withTestServer { (client, server) =>
        for {
          // Test non-existent database
          response <- client.get(GET / "database" / "non_existent_db")
          
          _ = response.status shouldBe org.http4s.Status.NotFound
        } yield ()
      }
    }
  }
  
  "Statistics integration" should {
    
    "work with real data" in {
      withTestServer { (client, server) =>
        for {
          // Create test data
          _ <- client.expect[String](POST / "test_db")
          _ <- client.expect[String](POST / "test_db" / "users")
          _ <- client.expect[String](POST / "test_db" / "users" / "user1" withEntity "value1")
          _ <- client.expect[String](POST / "test_db" / "users" / "user2" withEntity "value2")
          
          // Check stats reflect the data
          statsResponse <- client.expect[DatabaseStats](GET / "stats")
          _ = statsResponse.totalEntries should be >= 2
          
          dbResponse <- client.expect[DatabaseInfo](GET / "database" / "test_db")
          _ = dbResponse.totalEntries should be >= 2
          
        } yield ()
      }
    }
    
    "handle concurrent requests" in {
      withTestServer { (client, server) =>
        for {
          // Make concurrent requests
          statsFiber1 <- client.expect[DatabaseStats](GET / "stats").start
          statsFiber2 <- client.expect[DatabaseStats](GET / "stats").start
          statsFiber3 <- client.expect[DatabaseStats](GET / "stats").start
          
          stats1 <- statsFiber1.join
          stats2 <- statsFiber2.join
          stats3 <- statsFiber3.join
          
          _ = stats1.totalEntries should be(stats2.totalEntries)
          _ = stats2.totalEntries should be(stats3.totalEntries)
          _ = stats3.totalEntries should be(stats1.totalEntries)
        } yield ()
      }
    }
  }
  
  private def withTestServer[A](test: (Client[IO], Server[IO]) => IO[A]): IO[A] = {
    for {
      given Logger[IO] <- IO(Slf4jLogger.create[IO])
      
      // Create test server
      server <- DbInstance.resource(DbInstanceConfig(
        server = ServerConfig.Http(
          host = "localhost",
          port = 0, // Let system choose port
          idleTimeout = scala.concurrent.duration.Duration(30, "s"),
          maxConnections = 100
        ),
        engine = EngineConfig(
          rootDir = "/tmp/test-kvdb-stats",
          maxSegmentSize = 1024 * 1024,
          maxSegmentCount = 10,
          fileWriteBufferSize = 1024,
          fileWriteParallelism = 2
        )
      ))
      
      client <- EmberClientBuilder.default[IO].build
      
      result <- test(client, server)
      
    } yield result
  }
}
