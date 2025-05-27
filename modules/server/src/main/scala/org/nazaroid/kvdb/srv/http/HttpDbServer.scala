package org.nazaroid.kvdb.srv.http

import cats.effect.*
import cats.implicits.given
import com.comcast.ip4s.{Ipv4Address, Port}
import fs2.io.net.Network
import io.prometheus.client.CollectorRegistry
import org.http4s.*
import org.http4s.Status.Ok
import org.http4s.Uri.Path.Root
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.*
import org.http4s.metrics.prometheus.Prometheus
import org.http4s.server.*
import org.http4s.server.middleware.Metrics
import org.nazaroid.kvdb.algebra.DbServer
import org.nazaroid.kvdb.srv.DbSrvConf
import org.nazaroid.kvdb.srv.http.middlewares.Err
import org.typelevel.log4cats.Logger

final class HttpDbServer[F[_]: Async: Logger: Network](config: DbSrvConf) extends DbServer[F] with Err[F] {
  import dsl.*

  override def run(): F[Unit] = {
    val host = Ipv4Address
      .fromString(config.host)
      .getOrElse(throw new IllegalArgumentException(config.host))
    val port = Port
      .fromInt(config.port)
      .getOrElse(throw new IllegalArgumentException(config.port.toString))

    routes
      .flatMap(r =>
        EmberServerBuilder
          .default[F]
          .withHost(host)
          .withPort(port)
          .withHttpApp(r.orNotFound)
          .withIdleTimeout(config.idleTimeout)
          .withMaxConnections(config.maxConnections)
          .build
      )
      .useForever
      .as(ExitCode.Success)
      .map(_ => ())
  }

  private def routes: Resource[F, HttpRoutes[F]] =
    for {
      data   <- DataController()
      health <- HealthController()
    } yield Router(
      "/data"   -> data,
      "/health" -> health
    )

  private object dsl extends Http4sDsl[F]

  private object DataController {

    def apply(): Resource[F, HttpRoutes[F]] = {
      val healthService: HttpRoutes[F] = HttpRoutes.of[F] { case GET -> Root =>
        Ok("healthy")
      }
      for {
        healthServiceMetrics <- Prometheus
          .metricsOps[F](CollectorRegistry.defaultRegistry, "data")
      } yield Metrics[F](healthServiceMetrics)(withErrorLogging(healthService))
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
