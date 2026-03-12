package org.nazaroid.kvdb.examples

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.Files
import io.prometheus.client.CollectorRegistry
import org.http4s.HttpRoutes
import org.http4s.server.Router
import org.nazaroid.kvdb.bitcask.storage.StorageManager
import org.nazaroid.kvdb.statistics.{StatisticsIntegration, StatisticsService, MonitoringConfig}
import org.typelevel.log4cats.Logger

/**
 * Example of integrating StatisticsService with a microservice
 * that uses a shared Prometheus CollectorRegistry
 */
class MicroserviceIntegrationExample[F[_]: Async: Files: Logger](
  storageManager: StorageManager[F]
) {

  def createMicroserviceWithSharedMetrics(): Resource[F, HttpRoutes[F]] = {
    for {
      // Create shared Prometheus collector registry for the entire microservice
      collectorRegistry <- Resource.make(
        Async[F].delay(new CollectorRegistry())
      )(registry => Async[F].delay(registry.clear()))
      
      // Create statistics service
      statisticsService <- Resource.eval(
        StatisticsService.create(storageManager, MonitoringConfig())
      )
      
      // Create statistics integration
      statisticsIntegration <- Resource.eval(
        StatisticsIntegration.create(storageManager, MonitoringConfig())
      )
      
      // Register statistics metrics with the shared collector registry
      _ <- Resource.eval(
        statisticsIntegration.registerMetrics(collectorRegistry)
      )
      
      // Start background monitoring
      _ <- Resource.make(
        statisticsIntegration.startMonitoring()
      )(_ => statisticsIntegration.stopMonitoring())
      
      // Create HTTP routes for statistics
      statisticsRoutes <- Resource.eval(
        Async[F].pure(org.nazaroid.kvdb.statistics.StatisticsRoutes.create(statisticsIntegration))
      )
      
      // Create Prometheus metrics endpoint
      prometheusRoutes <- Resource.eval(
        Async[F].pure(org.http4s.metrics.prometheus.PrometheusExportService.build(collectorRegistry))
      )
      
      // Combine all routes
      allRoutes = Router(
        "/api" -> statisticsRoutes.routes,
        "/metrics" -> prometheusRoutes.routes
      )
      
    } yield allRoutes
  }
}

object MicroserviceIntegrationExample {
  
  /**
   * Example of how to set up a complete microservice with shared metrics
   */
  def setupCompleteMicroservice[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F],
    port: Int = 8080
  ): Resource[F, Unit] = {
    
    val microserviceExample = new MicroserviceIntegrationExample(storageManager)
    
    for {
      routes <- microserviceExample.createMicroserviceWithSharedMetrics()
      
      // Create HTTP server with shared metrics
      httpApp = org.http4s.ember.server.EmberServerBuilder[F]
        .withHost(org.http4s.ember.ServerHost.defaults)
        .withPort(port)
        .withHttpApp(routes)
        .build
      
      server <- httpApp.resource
      
    } yield ()
  }
  
  /**
   * Example of registering metrics with an existing collector registry
   */
  def registerWithExistingRegistry[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F],
    existingCollectorRegistry: CollectorRegistry
  ): F[Unit] = {
    for {
      statisticsService <- StatisticsService.create(storageManager, MonitoringConfig())
      statisticsIntegration <- StatisticsIntegration.create(storageManager, MonitoringConfig())
      
      // Register statistics metrics with the existing collector registry
      _ <- statisticsIntegration.registerMetrics(existingCollectorRegistry)
      
      // Start monitoring
      _ <- statisticsIntegration.startMonitoring()
      
    } yield ()
  }
  
  /**
   * Example of getting metrics programmatically
   */
  def getMetricsExample[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F]
  ): F[Unit] = {
    for {
      statisticsService <- StatisticsService.create(storageManager, MonitoringConfig())
      statisticsIntegration <- StatisticsIntegration.create(storageManager, MonitoringConfig())
      
      // Get all databases
      databases <- statisticsIntegration.getAllDatabases()
      _ <- Logger[F].info(s"Found ${databases.size} databases")
      
      // Get specific database stats
      dbStats <- statisticsIntegration.getDatabaseStats("test_db")
      _ <- dbStats match {
        case Some(stats) => 
          Logger[F].info(s"Database stats: entries=${stats.totalEntries}, active=${stats.activeEntries}")
        case None => 
          Logger[F].warn("Database not found")
      }
      
      // Get segment stats
      segmentStats <- statisticsIntegration.getSegmentStats("test_db")
      _ <- Logger[F].info(s"Found ${segmentStats.size} segments")
      
      // Get health check
      health <- statisticsIntegration.getHealthCheck()
      _ <- Logger[F].info(s"Health status: ${health.status}")
      
    } yield ()
  }
}
