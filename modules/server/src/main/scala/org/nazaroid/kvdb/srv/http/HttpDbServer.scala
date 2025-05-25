package org.nazaroid.kvdb.srv.http

import cats.effect.implicits.given
import cats.implicits.given
import cats.effect.{Async, Resource, unsafe}
import com.comcast.ip4s.{Ipv4Address, Port}
import io.prometheus.client.CollectorRegistry
import org.http4s.HttpRoutes
import org.http4s.Status.Ok
import org.http4s.Uri.Path.Root
import org.http4s.ember.server.EmberServerBuilder
import org.nazaroid.kvdb.algebra.{DbServer, DbServerHandle, DbSrvConf, ServerRuntime}
import org.typelevel.log4cats.Logger as String
import smithy4s.http.HttpMethod.GET
import org.http4s.metrics.prometheus.Prometheus
import org.http4s.*
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.*
import org.http4s.headers.`Content-Type`
import org.http4s.server.*
import cats.effect.*
import cats.syntax.all.*

class HttpDbServer[F[_]: Async: ServerRuntime](config: DbSrvConf) extends DbServer[F] {

  override def run(): F[DbServerHandle[F]] = {
    val host = Ipv4Address
      .fromString(config.host)
      .getOrElse(throw new IllegalArgumentException(config.host))
    val port = Port
      .fromInt(config.port)
      .getOrElse(throw new IllegalArgumentException(config.port.toString))

    val resource = routes.flatMap(r =>
      EmberServerBuilder
        .default[F]
        .withHost(host)
        .withPort(port)
        .withHttpApp(r.orNotFound)
        .withIdleTimeout(config.idleTimeout)
        .withMaxConnections(config.maxConnections)
        .build
    )

    new DbServerHandle[F] {
        override def stop(): F[Unit] = {
          CollectorRegistry.defaultRegistry.clear()
          Async[F].blocking(summon[ServerRuntime[F]].shutdown())
        }
      }.pure[F]
  }

  private def routes: Resource[F, HttpRoutes[F]] =
    for {
      data <- DataController()
      health <- HealthController()
    } yield Router(
      "/data" -> data,
      "/health" -> health
    )

  private object DataController {

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
