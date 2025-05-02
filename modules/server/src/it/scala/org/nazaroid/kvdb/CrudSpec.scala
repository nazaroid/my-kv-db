package org.nazaroid.kvdb.api

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.nazaroid.kvdb.{AppConfig, TestDbServerFactory}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

// noinspection ScalaUnusedSymbol
final class CrudSpec extends AnyFlatSpecLike {

  it should "`set` and `get` the same value" in {
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
    val appConfig = AppConfig()
    {
        val dbServerFactory = new TestDbServerFactory()
        for {
          dbSrv <- dbServerFactory.create(appConfig)
          db    <- dbSrv.createDatabase("db")
          tbl   <- db.createTable("tbl")
          _     <- tbl.set("key", "value")
          v     <- tbl.get("key")
        } yield {
          assert(v == "value")
        }

    }.unsafeRunSync()
  }
}

object Test {


}
