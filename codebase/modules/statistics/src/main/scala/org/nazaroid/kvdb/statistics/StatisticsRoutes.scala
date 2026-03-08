package org.nazaroid.kvdb.statistics

import cats.effect.Async
import cats.implicits.given
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import io.circe.generic.auto._
import io.circe.syntax._

class StatisticsRoutes[F[_]: Async](
  statisticsIntegration: StatisticsIntegration[F]
) extends Http4sDsl[F] {

  val routes: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "api" / "v1" / "stats" / "databases" =>
      for {
        databases <- statisticsIntegration.getAllDatabases()
        response <- Ok(databases.asJson)
      } yield response

    case GET -> Root / "api" / "v1" / "stats" / "databases" / dbName =>
      for {
        dbStats <- statisticsIntegration.getDatabaseStats(dbName)
        response <- dbStats match {
          case Some(stats) => Ok(stats.asJson)
          case None => NotFound(s"Database '$dbName' not found")
        }
      } yield response

    case GET -> Root / "api" / "v1" / "stats" / "databases" / dbName / "segments" =>
      for {
        segments <- statisticsIntegration.getSegmentStats(dbName)
        response <- Ok(segments.asJson)
      } yield response

    case GET -> Root / "api" / "v1" / "stats" / "prometheus" =>
      for {
        prometheus <- statisticsIntegration.exportForPrometheus()
        response <- Ok(prometheus, org.http4s.MediaType.text.plain)
      } yield response

    case GET -> Root / "api" / "v1" / "health" =>
      for {
        health <- statisticsIntegration.getHealthCheck()
        response <- Ok(health.asJson)
      } yield response

    case GET -> Root / "api" / "v1" / "stats" / "summary" =>
      for {
        databases <- statisticsIntegration.getAllDatabases()
        summary = DatabaseSummary(
          totalDatabases = databases.size,
          totalTables = databases.map(_.tables.size).sum,
          totalEntries = databases.map(_.totalEntries).sum,
          totalActiveEntries = databases.map(_.activeEntries).sum,
          totalDiskSize = databases.map(_.totalDiskSize).sum,
          totalMemorySize = databases.map(_.totalMemorySize).sum,
          averageFragmentation = if (databases.nonEmpty) {
            databases.map(_.fragmentationRatio).sum / databases.size
          } else 0.0,
          timestamp = System.currentTimeMillis()
        )
        response <- Ok(summary.asJson)
      } yield response
  }
}

case class DatabaseSummary(
  totalDatabases: Int,
  totalTables: Int,
  totalEntries: Int,
  totalActiveEntries: Int,
  totalDiskSize: Long,
  totalMemorySize: Long,
  averageFragmentation: Double,
  timestamp: Long
)

object StatisticsRoutes {
  def create[F[_]: Async](
    statisticsIntegration: StatisticsIntegration[F]
  ): StatisticsRoutes[F] = {
    new StatisticsRoutes(statisticsIntegration)
  }
}
