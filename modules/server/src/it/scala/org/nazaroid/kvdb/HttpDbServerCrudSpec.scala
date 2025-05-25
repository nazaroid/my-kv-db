package org.nazaroid.kvdb

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.http4s.Method.{GET, POST}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{EntityDecoder, Request, Uri}
import org.nazaroid.kvdb.algebra.DbSrvConf
import org.nazaroid.kvdb.srv.http.HttpDbServerFactory
import org.scalatest.flatspec.AnyFlatSpecLike
import org.typelevel.log4cats.slf4j.Slf4jLogger

// noinspection ScalaUnusedSymbol
final class HttpDbServerCrudSpec extends AnyFlatSpecLike {

  it should "`set` and `get` the same value" in {
    val responseDecoder: EntityDecoder[IO, String] = EntityDecoder.text

    val config = DbSrvConf()
    import config.*
    {
      val dbServerFactory = new HttpDbServerFactory()
      for {
        logger   <- Slf4jLogger.create[IO]
        srv      <- dbServerFactory.create(config)
        instance <- srv.run()

        req = Request[IO](POST, Uri.unsafeFromString(s"http://$host:$port/data/db"))
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
        _    <- instance.stop()
      } yield {
        assert(resp == "value")
      }

    }.unsafeRunSync()
  }
}

object Test {}
