package org.nazaroid.kvdb.srv.http

import cats.effect.{Async, Resource}
import cats.implicits.given
import com.comcast.ip4s.{Ipv4Address, Port}
import fs2.io.net.Network
import io.circe.generic.auto.*
import io.circe.syntax.*
import io.prometheus.client.CollectorRegistry
import org.http4s.Uri.Path.Root
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.metrics.prometheus.Prometheus
import org.http4s.server.Router
import org.http4s.server.middleware.Metrics
import org.http4s.{HttpRoutes, Request, Response}
import org.nazaroid.kvdb.core.*
import org.nazaroid.kvdb.srv.ServerConfig
import org.nazaroid.kvdb.srv.http.middlewares.Err
import org.typelevel.log4cats.Logger

final class HttpServer[F[_]: Async: Logger: Network](
  conf:              ServerConfig.Http,
  engine:            Engine[F],
  statisticsService: StatisticsService[F])
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
      stats  <- StatisticsController(statisticsService)
    } yield Router(
      "/data"   -> data,
      "/health" -> health,
      "/stats"  -> stats
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
                case Some(v) => Ok(v)
                case None    => NotFound("Value not found")
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

  private object StatisticsController {
    import org.http4s.circe.CirceEntityCodec.*

    def apply(statisticsService: StatisticsService[F]): Resource[F, HttpRoutes[F]] = {
      val statsService: HttpRoutes[F] = HttpRoutes.of[F] {

        // Get whole catalog stats
        case GET -> Root / "catalog" =>
          statisticsService.getStats.flatMap { stats =>
            Ok(stats.asJson)
          }

        // Get specific database stats
        case GET -> Root / "database" / dbName =>
          statisticsService.getDatabaseStats(dbName).flatMap {
            case Some(dbStats) => Ok(dbStats.asJson)
            case None          => NotFound(s"Database $dbName not found")
          }

        // Get specific table stats
        case GET -> Root / "table" / dbName / tableName =>
          statisticsService.getTableStats(dbName, tableName).flatMap {
            case Some(tableStats) => Ok(tableStats.asJson)
            case None             => NotFound(s"Table $tableName not found in database $dbName")
          }

      }
      for {
        statsServiceMetrics <- Prometheus
          .metricsOps[F](CollectorRegistry.defaultRegistry, "stats")
      } yield Metrics[F](statsServiceMetrics)(withErrorLogging(statsService))
    }
  }
}
