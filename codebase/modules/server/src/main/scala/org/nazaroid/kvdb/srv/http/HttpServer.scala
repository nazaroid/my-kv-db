package org.nazaroid.kvdb.srv.http

import cats.effect.{Async, Resource}
import cats.implicits.given
import com.comcast.ip4s.{Ipv4Address, Port}
import fs2.io.net.Network
import io.prometheus.client.CollectorRegistry
import org.http4s.Uri.Path.Root
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.metrics.prometheus.Prometheus
import org.http4s.server.Router
import org.http4s.server.middleware.Metrics
import org.http4s.{HttpRoutes, Request, Response, Status, Uri}
import org.nazaroid.kvdb.ServerConfig
import org.nazaroid.kvdb.algebra.{Engine, Server}
import org.nazaroid.kvdb.srv.http.middlewares.Err
import org.typelevel.log4cats.Logger

final class HttpServer[F[_]: Async: Logger: Network](conf: ServerConfig.Http, engine: Engine[F])
    extends Server[F]
    with Err[F] {

  import dsl.*

  def run(): Resource[F, Unit] = {
    val host = Ipv4Address
      .fromString(conf.host)
      .getOrElse(throw new IllegalArgumentException(conf.host))
    val port = Port
      .fromInt(conf.port)
      .getOrElse(throw new IllegalArgumentException(conf.port.toString))

    routes
      .flatMap { r =>
        EmberServerBuilder
          .default[F]
          .withHost(host)
          .withPort(port)
          .withHttpApp(r.orNotFound)
          .withIdleTimeout(conf.idleTimeout)
          .withMaxConnections(conf.maxConnections)
          .build
      }
      .map(_ => ())
  }

  private def routes: Resource[F, HttpRoutes[F]] =
    for {
      data   <- DataController(engine)
      health <- HealthController()
    } yield Router(
      "/data"   -> data,
      "/health" -> health
    )

  // noinspection ScalaStyle
  private object dsl extends Http4sDsl[F]

  private object DataController {

    def apply(engine: Engine[F]): Resource[F, HttpRoutes[F]] = {
      val dataService: HttpRoutes[F] = HttpRoutes.of[F] {
        // get value
        case GET -> Root / dbName / tblName / key =>
          {
            for {
              vOpt <- engine.get(dbName, tblName, key)
            } yield {
              vOpt match {
                case Some(v) => Status.Ok(v)
                case None    => Status.NotFound()
              }
            }
          }.flatten
        // set value
        case r @ POST -> Root / dbName / tblName / key =>
          {
            for {
              v <- r.bodyText.compile.fold("")(_ + _)
              _ <- engine.set(dbName, tblName, key, v)
            } yield Ok("OK")
          }.flatten
        // delete value
        case r @ DELETE -> Root / dbName / tblName / key =>
          {
            for {
              _ <- engine.delete(dbName, tblName, key)
            } yield Ok("OK")
          }.flatten
        // create tbl
        case POST -> Root / dbName / tblName =>
          {
            for {
              _ <- engine.createTableIfNotExists(dbName, tblName)
            } yield Ok("OK")
          }.flatten
        // create db
        case POST -> Root / dbName =>
          {
            for {
              _ <- engine.createDbIfNotExists(dbName)
            } yield Ok("OK")
          }.flatten
      }
      for {
        dataServiceMetrics <- Prometheus
          .metricsOps[F](CollectorRegistry.defaultRegistry, "data")
      } yield Metrics[F](dataServiceMetrics)(withErrorLogging(dataService))
    }

  }

  private object HealthController {

    def apply(): Resource[F, HttpRoutes[F]] = {
      val healthService: HttpRoutes[F] = HttpRoutes.of[F] { case GET -> Root =>
        Ok("healthy")
      }
      for {
        healthServiceMetrics <- Prometheus
          .metricsOps[F](CollectorRegistry.defaultRegistry, "health")
      } yield Metrics[F](healthServiceMetrics)(withErrorLogging(healthService))
    }
  }
}
