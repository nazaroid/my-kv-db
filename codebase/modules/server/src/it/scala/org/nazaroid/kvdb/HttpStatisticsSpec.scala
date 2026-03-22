package org.nazaroid.kvdb

import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import fs2.io.file.Files
import io.circe.Json
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.implicits.*
import org.nazaroid.kvdb.algebra.Engine
import org.nazaroid.kvdb.bitcask.BitcaskEngine
import org.nazaroid.kvdb.database.{DatabaseManager, DatabaseStats, DatabaseInfo, TableInfo}
import org.nazaroid.kvdb.srv.ServerConfig
import org.nazaroid.kvdb.srv.http.HttpServer
import org.nazaroid.kvdb.statistics.{StatisticsService, MonitoringConfig}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.*

import scala.concurrent.duration.*

object HttpStatisticsSpec extends IOSuite {

  type Res = (Client[IO], Engine[IO], StatisticsService[IO])

  def sharedResource: Resource[IO, (Client[IO], Engine[IO], StatisticsService[IO])] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[IO])
      tempDir <- Resource.eval(Files[IO].tempDirectory(None, "http-stats-test"))
      
      // Create Bitcask engine
      engine <- BitcaskEngine.create[IO](tempDir.toString)
      
      // Create statistics service
      statsService <- Resource.eval(
        StatisticsService.create[IO](engine.databaseManager, MonitoringConfig())
      )
      
      // Create HTTP server
      server <- new HttpServer[IO](
        conf = ServerConfig.Http(
          host = "localhost",
          port = 0, // Random port
          idleTimeout = 30.seconds,
          maxConnections = 10
        ),
        engine = engine,
        statisticsService = statsService
      ).run().background
      
      // Create HTTP client
      client <- EmberClientBuilder.default[IO].build
      
      // Wait for server to start
      _ <- Resource.eval(IO.sleep(1.second))
      
    } yield (client, engine, statsService)

  test("GET /stats/catalog should return catalog statistics") { case (client, engine, statsService) =>
    for {
      // Setup: create some test data
      _ <- engine.set("testdb", "users", "user1", "John Doe")
      _ <- engine.set("testdb", "users", "user2", "Jane Smith")
      _ <- engine.set("testdb", "orders", "order1", "product1")
      
      // Test catalog statistics
      request = Request[IO](Method.GET, uri"http://localhost:8080/stats/catalog")
      response <- client.expect[Json](request)
      
      // Verify catalog stats structure
      _ <- IO {
        assert(response.isObject, "Catalog stats should be a JSON object")
        val obj = response.asObject.get
        assert(obj.contains("totalDatabases"), "Should contain totalDatabases")
        assert(obj.contains("totalTables"), "Should contain totalTables")
        assert(obj.contains("totalEntries"), "Should contain totalEntries")
        assert(obj.contains("activeEntries"), "Should contain activeEntries")
        assert(obj.contains("deletedEntries"), "Should contain deletedEntries")
        assert(obj.contains("totalDataSize"), "Should contain totalDataSize")
        assert(obj.contains("details"), "Should contain details")
      }
      
      // Verify values
      _ <- IO {
        val obj = response.asObject.get
        assert(obj("totalDatabases").exists(_.asNumber.exists(_.toInt == 1)), "Should have 1 database")
        assert(obj("totalTables").exists(_.asNumber.exists(_.toInt == 2)), "Should have 2 tables")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 entries")
        assert(obj("activeEntries").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 active entries")
        assert(obj("deletedEntries").exists(_.asNumber.exists(_.toInt == 0)), "Should have 0 deleted entries")
      }
      
    } yield success
  }

  test("GET /stats/database/{dbName} should return database statistics") { case (client, engine, statsService) =>
    for {
      // Setup: create test data
      _ <- engine.set("testdb", "users", "user1", "John Doe")
      _ <- engine.set("testdb", "users", "user2", "Jane Smith")
      _ <- engine.set("testdb", "orders", "order1", "product1")
      
      // Test database statistics
      request = Request[IO](Method.GET, uri"http://localhost:8080/stats/database/testdb")
      response <- client.expect[Json](request)
      
      // Verify database stats structure
      _ <- IO {
        assert(response.isObject, "Database stats should be a JSON object")
        val obj = response.asObject.get
        assert(obj.contains("name"), "Should contain name")
        assert(obj.contains("totalTables"), "Should contain totalTables")
        assert(obj.contains("totalEntries"), "Should contain totalEntries")
        assert(obj.contains("activeEntries"), "Should contain activeEntries")
        assert(obj.contains("deletedEntries"), "Should contain deletedEntries")
        assert(obj.contains("totalDataSize"), "Should contain totalDataSize")
        assert(obj.contains("details"), "Should contain details")
      }
      
      // Verify values
      _ <- IO {
        val obj = response.asObject.get
        assert(obj("name").exists(_.asString.contains("testdb")), "Should have correct database name")
        assert(obj("totalTables").exists(_.asNumber.exists(_.toInt == 2)), "Should have 2 tables")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 entries")
        assert(obj("activeEntries").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 active entries")
        assert(obj("deletedEntries").exists(_.asNumber.exists(_.toInt == 0)), "Should have 0 deleted entries")
      }
      
      // Verify engine-specific details
      _ <- IO {
        val obj = response.asObject.get
        assert(obj("details").exists(_.isObject), "Details should be an object")
        val details = obj("details").get.asObject.get
        assert(details.contains("engine_specific"), "Should contain engine_specific details")
        assert(details("engine_specific").exists(_.isObject), "Engine specific should be an object")
      }
      
    } yield success
  }

  test("GET /stats/table/{dbName}/{tableName} should return table statistics") { case (client, engine, statsService) =>
    for {
      // Setup: create test data
      _ <- engine.set("testdb", "users", "user1", "John Doe")
      _ <- engine.set("testdb", "users", "user2", "Jane Smith")
      _ <- engine.set("testdb", "orders", "order1", "product1")
      
      // Test table statistics
      request = Request[IO](Method.GET, uri"http://localhost:8080/stats/table/testdb/users")
      response <- client.expect[Json](request)
      
      // Verify table stats structure
      _ <- IO {
        assert(response.isObject, "Table stats should be a JSON object")
        val obj = response.asObject.get
        assert(obj.contains("name"), "Should contain name")
        assert(obj.contains("totalEntries"), "Should contain totalEntries")
        assert(obj.contains("activeEntries"), "Should contain activeEntries")
        assert(obj.contains("deletedEntries"), "Should contain deletedEntries")
        assert(obj.contains("totalDataSize"), "Should contain totalDataSize")
        assert(obj.contains("details"), "Should contain details")
      }
      
      // Verify values
      _ <- IO {
        val obj = response.asObject.get
        assert(obj("name").exists(_.asString.contains("users")), "Should have correct table name")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 2)), "Should have 2 entries")
        assert(obj("activeEntries").exists(_.asNumber.exists(_.toInt == 2)), "Should have 2 active entries")
        assert(obj("deletedEntries").exists(_.asNumber.exists(_.toInt == 0)), "Should have 0 deleted entries")
      }
      
      // Verify engine-specific details with segments
      _ <- IO {
        val obj = response.asObject.get
        assert(obj("details").exists(_.isObject), "Details should be an object")
        val details = obj("details").get.asObject.get
        assert(details.contains("engine_specific"), "Should contain engine_specific details")
        
        val engineSpecific = details("engine_specific").get.asObject.get
        assert(engineSpecific.contains("segments"), "Should contain segments information")
        assert(engineSpecific("segments").exists(_.isArray), "Segments should be an array")
      }
      
    } yield success
  }

  test("GET /stats/database/{nonExistentDb} should return 404") { case (client, engine, statsService) =>
    for {
      request = Request[IO](Method.GET, uri"http://localhost:8080/stats/database/nonexistent")
      response <- client.status(request)
      
      _ <- IO {
        assert(response == Status.NotFound, "Should return 404 for non-existent database")
      }
      
    } yield success
  }

  test("GET /stats/table/{dbName}/{nonExistentTable} should return 404") { case (client, engine, statsService) =>
    for {
      // Setup: create database but no table
      _ <- engine.set("testdb", "users", "user1", "John Doe")
      
      request = Request[IO](Method.GET, uri"http://localhost:8080/stats/table/testdb/nonexistent")
      response <- client.status(request)
      
      _ <- IO {
        assert(response == Status.NotFound, "Should return 404 for non-existent table")
      }
      
    } yield success
  }

  test("Statistics should reflect data changes") { case (client, engine, statsService) =>
    for {
      // Initial setup
      _ <- engine.set("testdb", "users", "user1", "John Doe")
      
      // Get initial stats
      initialRequest = Request[IO](Method.GET, uri"http://localhost:8080/stats/table/testdb/users")
      initialResponse <- client.expect[Json](initialRequest)
      
      _ <- IO {
        val obj = initialResponse.asObject.get
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 1)), "Should have 1 entry initially")
      }
      
      // Add more data
      _ <- engine.set("testdb", "users", "user2", "Jane Smith")
      _ <- engine.set("testdb", "users", "user3", "Bob Johnson")
      
      // Get updated stats
      updatedRequest = Request[IO](Method.GET, uri"http://localhost:8080/stats/table/testdb/users")
      updatedResponse <- client.expect[Json](updatedRequest)
      
      _ <- IO {
        val obj = updatedResponse.asObject.get
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 entries after updates")
      }
      
      // Test deletion
      _ <- engine.delete("testdb", "users", "user2")
      
      // Get final stats
      finalRequest = Request[IO](Method.GET, uri"http://localhost:8080/stats/table/testdb/users")
      finalResponse <- client.expect[Json](finalRequest)
      
      _ <- IO {
        val obj = finalResponse.asObject.get
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 2)), "Should have 2 entries after deletion")
        assert(obj("deletedEntries").exists(_.asNumber.exists(_.toInt == 1)), "Should have 1 deleted entry")
      }
      
    } yield success
  }

  test("Multiple databases statistics") { case (client, engine, statsService) =>
    for {
      // Setup: create multiple databases
      _ <- engine.set("db1", "users", "user1", "John Doe")
      _ <- engine.set("db1", "orders", "order1", "product1")
      _ <- engine.set("db2", "products", "prod1", "Laptop")
      _ <- engine.set("db3", "customers", "cust1", "Alice")
      
      // Test catalog statistics
      catalogRequest = Request[IO](Method.GET, uri"http://localhost:8080/stats/catalog")
      catalogResponse <- client.expect[Json](catalogRequest)
      
      _ <- IO {
        val obj = catalogResponse.asObject.get
        assert(obj("totalDatabases").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 databases")
        assert(obj("totalTables").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 tables")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 4)), "Should have 4 total entries")
      }
      
      // Test individual database stats
      db1Request = Request[IO](Method.GET, uri"http://localhost:8080/stats/database/db1")
      db1Response <- client.expect[Json](db1Request)
      
      _ <- IO {
        val obj = db1Response.asObject.get
        assert(obj("totalTables").exists(_.asNumber.exists(_.toInt == 2)), "DB1 should have 2 tables")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 2)), "DB1 should have 2 entries")
      }
      
      db2Request = Request[IO](Method.GET, uri"http://localhost:8080/stats/database/db2")
      db2Response <- client.expect[Json](db2Request)
      
      _ <- IO {
        val obj = db2Response.asObject.get
        assert(obj("totalTables").exists(_.asNumber.exists(_.toInt == 1)), "DB2 should have 1 table")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 1)), "DB2 should have 1 entry")
      }
      
    } yield success
  }
}
