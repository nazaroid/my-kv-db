package org.nazaroid.kvdb.api

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.IORuntime
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.composition.DiContainer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

// noinspection ScalaUnusedSymbol
final class CrudSpec extends AnyFlatSpecLike{

  it should "`set` and `get` the same value" in {
    val di = new DiContainer[IO]()
    for {
      dbSrv <- di.resolveDbServer()
      db <- dbSrv.createDatabase("db")
      tbl <- db.createTable("tbl")
      _ <- tbl.set("key", "value")
      v <- tbl.get("key")
    } yield {
      assert(v == "value")
    }

  }
}

