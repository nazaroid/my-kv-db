package org.nazaroid.kvdb.examples

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.Files
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.bitcask.storage.StorageManager
import org.nazaroid.kvdb.statistics.{MetricsAdapter, StatisticsIntegration, StatisticsService, MonitoringConfig}
import org.typelevel.log4cats.Logger

/**
 * Examples of using different metrics adapters with StatisticsService
 */
class AdapterIntegrationExample[F[_]: Async: Files: Logger](
  storageManager: StorageManager[F]
) {

  /** Example 1: Using Prometheus adapter */
  def setupWithPrometheus(): Resource[F, Unit] = {
    for {
      statisticsService <- Resource.eval(StatisticsService.create(storageManager, MonitoringConfig()))
      statisticsIntegration <- Resource.eval(StatisticsIntegration.create(storageManager, MonitoringConfig()))
      
      // Create shared Prometheus collector registry
      collectorRegistry <- Resource.make(
        Async[F].delay(new CollectorRegistry())
      )(registry => Async[F].delay(registry.clear()))
      
      // Register with Prometheus adapter
      _ <- Resource.eval(
        statisticsIntegration.registerMetrics(collectorRegistry)
      )
      
      // Start monitoring
      _ <- Resource.make(
        statisticsIntegration.startMonitoring()
      )(_ => statisticsIntegration.stopMonitoring())
      
    } yield ()
  }

  /** Example 2: Using custom metrics adapter */
  def setupWithCustomAdapter(): Resource[F, Unit] = {
    for {
      statisticsService <- Resource.eval(StatisticsService.create(storageManager, MonitoringConfig()))
      
      // Create custom adapter (e.g., for InfluxDB, Datadog, etc.)
      customAdapter <- Resource.eval(Async[F].delay(
        new CustomMetricsAdapter[F]() // Would be implemented separately
      ))
      
      // Register with custom adapter
      _ <- Resource.eval(
        statisticsService.setMetricsAdapter(customAdapter)
      )
      
      _ <- Resource.eval(
        statisticsService.registerMetrics()
      )
      
    } yield ()
  }

  /** Example 3: Using no-op adapter for testing */
  def setupForTesting(): Resource[F, Unit] = {
    for {
      statisticsService <- Resource.eval(StatisticsService.create(storageManager, MonitoringConfig()))
      
      // Use no-op adapter for unit tests
      noOpAdapter <- Resource.eval(Async[F].delay(
        MetricsAdapter.createNoOpAdapter[F]()
      ))
      
      _ <- Resource.eval(
        statisticsService.setMetricsAdapter(noOpAdapter)
      )
      
      _ <- Resource.eval(
        statisticsService.registerMetrics()
      )
      
    } yield ()
  }

  /** Example 4: Dynamic adapter switching */
  def setupWithDynamicSwitching(): F[Unit] = {
    for {
      statisticsService <- StatisticsService.create(storageManager, MonitoringConfig())
      
      // Start with no-op adapter
      noOpAdapter = MetricsAdapter.createNoOpAdapter[F]()
      _ <- statisticsService.setMetricsAdapter(noOpAdapter)
      _ <- statisticsService.registerMetrics()
      
      // Later switch to Prometheus
      collectorRegistry <- Async[F].delay(new CollectorRegistry())
      prometheusAdapter = MetricsAdapter.createPrometheusAdapter(collectorRegistry)
      _ <- statisticsService.setMetricsAdapter(prometheusAdapter)
      _ <- statisticsService.registerMetrics()
      
      _ <- Logger[F].info("Switched from no-op to Prometheus adapter")
      
    } yield ()
  }
}

/** Example custom metrics adapter for other systems */
class CustomMetricsAdapter[F[_]: Async: Logger] extends MetricsAdapter[F] {
  
  override def registerDatabaseMetrics(): F[Unit] = {
    Logger[F].info("Registering database metrics with custom system")
    // Implementation for custom metrics system (e.g., InfluxDB, Datadog)
    Async[F].unit
  }

  override def registerTableMetrics(): F[Unit] = {
    Logger[F].info("Registering table metrics with custom system")
    Async[F].unit
  }

  override def registerSegmentMetrics(): F[Unit] = {
    Logger[F].info("Registering segment metrics with custom system")
    Async[F].unit
  }

  override def updateDatabaseMetrics(databases: List[DatabaseInfo]): F[Unit] = {
    databases.traverse_ { db =>
      Logger[F].info(s"Updating custom database metrics for ${db.name}: ${db.totalEntries} entries")
      // Send to custom metrics system
      Async[F].unit
    }
  }

  override def updateTableMetrics(databases: List[DatabaseInfo]): F[Unit] = {
    databases.traverse_ { db =>
      db.tables.traverse_ { table =>
        Logger[F].info(s"Updating custom table metrics for ${db.name}.${table.name}: ${table.entryCount} entries")
        // Send to custom metrics system
        Async[F].unit
      }
    }
  }

  override def updateSegmentMetrics(databases: List[DatabaseInfo]): F[Unit] = {
    databases.traverse_ { db =>
      Logger[F].info(s"Updating custom segment metrics for ${db.name}")
      // Send to custom metrics system
      Async[F].unit
    }
  }
}

object AdapterIntegrationExample {
  
  /**
   * Complete microservice setup with adapter pattern
   */
  def setupMicroserviceWithAdapter[F[_]: Async: Files: Logger](
    storageManager: StorageManager[F],
    adapterType: String = "prometheus"
  ): Resource[F, Unit] = {
    
    val example = new AdapterIntegrationExample(storageManager)
    
    adapterType.toLowerCase match {
      case "prometheus" => example.setupWithPrometheus()
      case "custom" => example.setupWithCustomAdapter()
      case "test" => example.setupForTesting()
      case _ => 
        Resource.raiseError(new IllegalArgumentException(s"Unknown adapter type: $adapterType"))
    }
  }
  
  /**
   * Example of creating adapters programmatically
   */
  def createAdapter[F[_]: Async: Logger](
    adapterType: String,
    collectorRegistry: Option[CollectorRegistry] = None
  ): MetricsAdapter[F] = {
    adapterType.toLowerCase match {
      case "prometheus" => 
        collectorRegistry match {
          case Some(registry) => MetricsAdapter.createPrometheusAdapter(registry)
          case None => throw new IllegalArgumentException("Collector registry required for Prometheus adapter")
        }
      case "noop" => MetricsAdapter.createNoOpAdapter()
      case "custom" => new CustomMetricsAdapter()
      case _ => throw new IllegalArgumentException(s"Unknown adapter type: $adapterType")
    }
  }
}
