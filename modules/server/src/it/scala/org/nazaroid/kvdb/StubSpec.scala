package org.nazaroid.kvdb.api

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.IORuntime
import fs2.io.net.Network
import io.prometheus.client.CollectorRegistry
import org.http4s.Method.GET
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, EntityDecoder, Request, Uri}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

// noinspection ScalaUnusedSymbol
final class StubSpec extends AnyFlatSpecLike{



  it should "success" in {
    ???
  }
}

