package org.nazaroid.kvdb

import cats.effect.IO
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.effect.testing.scalatest.AsyncIOSpec
import org.http4s.Method.{DELETE, GET, POST}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{EntityDecoder, Request, Status, Uri}
import org.nazaroid.kvdb.srv.{DbInstance, DbInstanceConfig, ServerConfig}
import org.scalatest.FutureOutcome
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}

import java.nio.file.Paths
import scala.reflect.io.Directory

// noinspection ScalaUnusedSymbol
final class HttpDbServerCrudSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

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
  private val host = httpConf.host
  private val port = httpConf.port
  private val responseDecoder: EntityDecoder[IO, String] = EntityDecoder.text

  "should `set` and `get` the same value" in {
    Dispatcher.parallel[IO] use { d =>
      given Dispatcher[IO] = d

      for {
        logger <- Slf4jLogger.create[IO]
        given Logger[IO] = logger

        _ <- DbInstance[IO]().resource(config).use { handle =>
          for {
            req  <- Async[IO].blocking(Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db")))
            _    <- logger.info(f"create db request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"create db response: $resp")

            req = Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl"))
            _    <- logger.info(f"create tbl request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"create tbl response: $resp")

            req = Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl/key")).withEntity("value")
            _    <- logger.info(f"set value request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"set value response: $resp")

            req = Request[IO](GET, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl/key"))
            _    <- logger.info(f"get value request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"get value response: $resp")
            _    <- handle.stop
          } yield assert(resp == "value")
        }
      } yield ()
    }
  }

  "can `delete` value after `set`" in {
    Dispatcher.parallel[IO] use { d =>
      given Dispatcher[IO] = d

      for {
        logger <- Slf4jLogger.create[IO]
        given Logger[IO] = logger

        _ <- DbInstance[IO]().resource(config).use { handle =>
          for {
            req  <- Async[IO].blocking(Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db")))
            _    <- logger.info(f"create db request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"create db response: $resp")

            req = Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl"))
            _    <- logger.info(f"create tbl request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"create tbl response: $resp")

            req = Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl/key")).withEntity("value")
            _    <- logger.info(f"set value request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"set value response: $resp")

            req = Request[IO](DELETE, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl/key"))
            _    <- logger.info(f"delete value request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"delete value response: $resp")

            req = Request[IO](GET, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl/key"))
            _    <- logger.info(f"get value request: $req")
            status <- EmberClientBuilder.default[IO].build.use(_.status(req))
            _    <- logger.info(f"get value response: $resp")
            _    <- handle.stop
          } yield assert(status == Status.NotFound)
        }
      } yield ()
    }
  }

  "can `get` value after db runtime restart" in {
    Dispatcher.parallel[IO] use { d =>
      given Dispatcher[IO] = d

      for {
        logger <- Slf4jLogger.create[IO]
        given Logger[IO] = logger
        _ <- logger.info(f"=== first db session ===")
        _ <- DbInstance[IO]().resource(config).use { handle =>
          for {
            req  <- Async[IO].blocking(Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db")))
            _    <- logger.info(f"create db request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"create db response: $resp")

            req = Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl"))
            _    <- logger.info(f"create tbl request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"create tbl response: $resp")

            req = Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl/key")).withEntity("value")
            _    <- logger.info(f"set value request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"set value response: $resp")
            _    <- handle.stop
          } yield ()
        }
        _ <- logger.info(f"=== second db session ===")
        _ <- DbInstance[IO]().resource(config).use { handle =>
          for {
            req  <- Async[IO].blocking(Request[IO](GET, Uri.unsafeFromString(s"http://$host:$port/data/db/tbl/key")))
            _    <- logger.info(f"get value request: $req")
            resp <- EmberClientBuilder.default[IO].build.use(_.expect(req)(responseDecoder))
            _    <- logger.info(f"get value response: $resp")
            _    <- handle.stop
          } yield assert(resp == "value")
        }
      } yield ()
    }
  }
}
