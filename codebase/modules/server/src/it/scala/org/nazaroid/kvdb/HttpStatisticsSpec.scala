package org.nazaroid.kvdb

import cats.effect.implicits.given
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.given
import fs2.io.file.Files
import io.circe.Json
import org.http4s.Method.{DELETE, POST}
import org.http4s.circe.*
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.implicits.*
import org.http4s.{EntityDecoder, Method, Request, Status, Uri}
import org.nazaroid.kvdb.bitcask.BitcaskEngine
import org.nazaroid.kvdb.core.*
import org.nazaroid.kvdb.srv.http.HttpServer
import org.nazaroid.kvdb.srv.{DbInstance, DbInstanceConfig, ServerConfig}
import org.scalatest.FutureOutcome
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.http4s.implicits.given

import java.nio.file.Paths
import scala.concurrent.duration.*
import scala.reflect.io.Directory

final class HttpStatisticsSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    java.nio.file.Files.createDirectories(testDir)
    val outcome = super.withFixture(test)
    outcome.onCompletedThen { _ =>
      val dir = new Directory(testDir.toFile)
      if (dir.exists) {
        dir.deleteRecursively()
      }
    }
  }

  private val testDir = Paths.get("./testFolder")
  private val config  = DbInstanceConfig()

  private val httpConf = config.server match {
    case http: ServerConfig.Http => http
    case _                       => throw new IllegalStateException("Expected Http server configuration")
  }
  private val host    = httpConf.host
  private val port    = httpConf.port
  private val baseUrl = s"http://$host:$port"
  private val responseDecoder: EntityDecoder[IO, String] = EntityDecoder.text

  def withDbServerRunning[T](test: Client[IO] => IO[T]): IO[T] = {
    Dispatcher.parallel[IO].use { d =>
      given Dispatcher[IO] = d
      for {
        logger <- Slf4jLogger.create[IO]
        given Logger[IO] = logger
        res <- DbInstance[IO]().resource(config).use { handle =>
          EmberClientBuilder.default[IO].build.use { client =>
            test(client)
          }
        }
      } yield res
    }
  }

  "GET /stats/catalog should return catalog statistics" in withDbServerRunning { client =>
    for {
      // Setup: create some test data
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user1")).withEntity("John Doe")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user2")).withEntity("John Smith")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/orders/order1")).withEntity("product1")).use_

      // Test catalog statistics
      request = Request[IO](Method.GET, uri"$baseUrl/stats/catalog")
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

  "GET /stats/database/{dbName} should return database statistics" in withDbServerRunning { client =>
    for {
      // Setup: create test data
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user1")).withEntity("John Doe")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user2")).withEntity("John Smith")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/orders/order1")).withEntity("product1")).use_

      // Test database statistics
      request = Request[IO](Method.GET, uri"$baseUrl/stats/database/testdb")
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

  "GET /stats/table/{dbName}/{tableName} should return table statistics" in withDbServerRunning { client =>
    for {
      // Setup: create test data
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user1")).withEntity("John Doe")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user2")).withEntity("John Smith")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/orders/order1")).withEntity("product1")).use_

      // Test table statistics
      request = Request[IO](Method.GET, uri"$baseUrl/stats/table/testdb/users")
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

  "GET /stats/database/{nonExistentDb} should return 404" in withDbServerRunning { client =>
    for {
      request  <- Request[IO](Method.GET, uri"$baseUrl/stats/database/nonexistent").pure[IO]
      response <- client.status(request)

      _ <- IO {
        assert(response == Status.NotFound, "Should return 404 for non-existent database")
      }

    } yield success
  }

  "GET /stats/table/{dbName}/{nonExistentTable} should return 404" in withDbServerRunning { client =>
    for {
      // Setup: create database but no table
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user1")).withEntity("John Doe")).use_

      request = Request[IO](Method.GET, uri"$baseUrl/stats/table/testdb/nonexistent")
      response <- client.status(request)

      _ <- IO {
        assert(response == Status.NotFound, "Should return 404 for non-existent table")
      }

    } yield success
  }

  "Statistics should reflect data changes" in withDbServerRunning { client =>
    for {
      // Initial setup
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user1")).withEntity("John Doe")).use_

      // Get initial stats
      initialRequest = Request[IO](Method.GET, uri"$baseUrl/stats/table/testdb/users")
      initialResponse <- client.expect[Json](initialRequest)

      _ <- IO {
        val obj = initialResponse.asObject.get
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 1)), "Should have 1 entry initially")
      }

      // Add more data
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user3")).withEntity("Jane Smith")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user2")).withEntity("Bob Johnson")).use_

      // Get updated stats
      updatedRequest = Request[IO](Method.GET, uri"$baseUrl/stats/table/testdb/users")
      updatedResponse <- client.expect[Json](updatedRequest)

      _ <- IO {
        val obj = updatedResponse.asObject.get
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 entries after updates")
      }

      // Test deletion
      _ <- client.run(Request[IO](Request[IO](DELETE, uri(f"$baseUrl/data/testdb/users/user2")))).use_

      // Get final stats
      finalRequest = Request[IO](Method.GET, uri"$baseUrl/stats/table/testdb/users")
      finalResponse <- client.expect[Json](finalRequest)

      _ <- IO {
        val obj = finalResponse.asObject.get
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 2)), "Should have 2 entries after deletion")
        assert(obj("deletedEntries").exists(_.asNumber.exists(_.toInt == 1)), "Should have 1 deleted entry")
      }

    } yield success
  }

  "Multiple databases statistics" in withDbServerRunning { client =>
    for {
      // Setup: create multiple databases
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/testdb/users/user1")).withEntity("John Doe")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/db1/orders/order1")).withEntity("product1")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/db2/products/prod1")).withEntity("Laptop")).use_
      _ <- client.run(Request[IO](POST, uri(f"$baseUrl/data/db3/customers/cust1")).withEntity("Alice")).use_

      // Test catalog statistics
      catalogRequest = Request[IO](Method.GET, uri"$baseUrl/stats/catalog")
      catalogResponse <- client.expect[Json](catalogRequest)

      _ <- IO {
        val obj = catalogResponse.asObject.get
        assert(obj("totalDatabases").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 databases")
        assert(obj("totalTables").exists(_.asNumber.exists(_.toInt == 3)), "Should have 3 tables")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 4)), "Should have 4 total entries")
      }

      // Test individual database stats
      db1Request = Request[IO](Method.GET, uri"$baseUrl/stats/database/db1")
      db1Response <- client.expect[Json](db1Request)

      _ <- IO {
        val obj = db1Response.asObject.get
        assert(obj("totalTables").exists(_.asNumber.exists(_.toInt == 2)), "DB1 should have 2 tables")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 2)), "DB1 should have 2 entries")
      }

      db2Request = Request[IO](Method.GET, uri"$baseUrl/stats/database/db2")
      db2Response <- client.expect[Json](db2Request)

      _ <- IO {
        val obj = db2Response.asObject.get
        assert(obj("totalTables").exists(_.asNumber.exists(_.toInt == 1)), "DB2 should have 1 table")
        assert(obj("totalEntries").exists(_.asNumber.exists(_.toInt == 1)), "DB2 should have 1 entry")
      }

    } yield success
  }
}
