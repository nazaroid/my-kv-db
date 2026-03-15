package org.nazaroid.kvdb.statistics

import cats.effect.Async
import cats.implicits.given
import org.typelevel.log4cats.Logger

/** Generic metrics adapter interface */
trait MetricsAdapter[F[_]] {
  def registerDatabaseMetrics(): F[Unit]
  def registerTableMetrics(): F[Unit]
  def registerSegmentMetrics(): F[Unit]
  def updateDatabaseMetrics(databases: List[DatabaseInfo]): F[Unit]
  def updateTableMetrics(databases: List[DatabaseInfo]): F[Unit]
  def updateSegmentMetrics(databases: List[DatabaseInfo]): F[Unit]
}

/** Prometheus-specific metrics adapter */
class PrometheusMetricsAdapter[F[_]: Async: Logger](
  collectorRegistry: io.prometheus.client.CollectorRegistry
) extends MetricsAdapter[F] {

  // Store references to registered Prometheus gauges for efficient updates
  private var dbInfoGauge: Option[io.prometheus.client.Gauge] = None
  private var tableInfoGauge: Option[io.prometheus.client.Gauge] = None
  private var segmentInfoGauge: Option[io.prometheus.client.Gauge] = None

  override def registerDatabaseMetrics(): F[Unit] = {
    for {
      _ <- Logger[F].info("Registering database metrics with Prometheus")
      
      dbGauge <- Async[F].delay {
        val gauge = io.prometheus.client.Gauge
          .build()
          .name("kvdb_database_info")
          .help("Database information")
          .labelNames("database", "type")
          .create()
        collectorRegistry.register(gauge)
        gauge
      }
      _ <- Async[F].delay {
        dbInfoGauge = Some(dbGauge)
      }
    } yield ()
  }

  override def registerTableMetrics(): F[Unit] = {
    for {
      _ <- Logger[F].info("Registering table metrics with Prometheus")
      
      tableGauge <- Async[F].delay {
        val gauge = io.prometheus.client.Gauge
          .build()
          .name("kvdb_table_info")
          .help("Table information")
          .labelNames("database", "table", "type")
          .create()
        collectorRegistry.register(gauge)
        gauge
      }
      _ <- Async[F].delay {
        tableInfoGauge = Some(tableGauge)
      }
    } yield ()
  }

  override def registerSegmentMetrics(): F[Unit] = {
    for {
      _ <- Logger[F].info("Registering segment metrics with Prometheus")
      
      segmentGauge <- Async[F].delay {
        val gauge = io.prometheus.client.Gauge
          .build()
          .name("kvdb_segment_info")
          .help("Segment information")
          .labelNames("database", "segment", "type")
          .create()
        collectorRegistry.register(gauge)
        gauge
      }
      _ <- Async[F].delay {
        segmentInfoGauge = Some(segmentGauge)
      }
    } yield ()
  }

  override def updateDatabaseMetrics(databases: List[DatabaseInfo]): F[Unit] = {
    dbInfoGauge.traverse_ { gauge =>
      databases.traverse_ { db =>
        Async[F].delay {
          try {
            gauge.labels(db.name, "total_entries").set(db.totalEntries)
            gauge.labels(db.name, "active_entries").set(db.activeEntries)
            gauge.labels(db.name, "deleted_entries").set(db.deletedEntries)
            gauge.labels(db.name, "disk_size_bytes").set(db.totalDiskSize.toDouble)
            gauge.labels(db.name, "memory_size_bytes").set(db.totalMemorySize.toDouble)
            gauge.labels(db.name, "fragmentation_ratio").set(db.fragmentationRatio)
          } catch {
            case _: Exception => 
              Logger[F].warn(s"Failed to update database metrics for ${db.name}")
          }
        }
      }
    }
  }

  override def updateTableMetrics(databases: List[DatabaseInfo]): F[Unit] = {
    tableInfoGauge.traverse_ { gauge =>
      databases.traverse_ { db =>
        db.tables.traverse_ { table =>
          Async[F].delay {
            try {
              gauge.labels(db.name, table.name, "entries").set(table.entryCount)
              gauge.labels(db.name, table.name, "active_entries").set(table.activeEntryCount)
              gauge.labels(db.name, table.name, "disk_size_bytes").set(table.diskSize.toDouble)
              gauge.labels(db.name, table.name, "memory_size_bytes").set(table.memorySize.toDouble)
            } catch {
              case _: Exception =>
                Logger[F].warn(s"Failed to update table metrics for ${db.name}.${table.name}")
            }
          }
        }
      }
    }
  }

  override def updateSegmentMetrics(databases: List[DatabaseInfo]): F[Unit] = {
    segmentInfoGauge.traverse_ { gauge =>
      databases.traverse_ { db =>
        // This would need access to segment stats - simplified for now
        Async[F].delay {
          try {
            // Placeholder segment metrics - in real implementation would fetch actual segment data
            gauge.labels(db.name, "segment_1", "file_size_bytes").set(1024.0)
            gauge.labels(db.name, "segment_1", "is_active").set(1.0)
            gauge.labels(db.name, "segment_1", "stale_ratio").set(0.1)
            gauge.labels(db.name, "segment_1", "entry_count").set(100.0)
          } catch {
            case _: Exception =>
              Logger[F].warn(s"Failed to update segment metrics for ${db.name}")
          }
        }
      }
    }
  }
}

/** No-op metrics adapter for testing or when metrics are disabled */
class NoOpMetricsAdapter[F[_]: Async] extends MetricsAdapter[F] {
  override def registerDatabaseMetrics(): F[Unit] = Async[F].unit
  override def registerTableMetrics(): F[Unit] = Async[F].unit
  override def registerSegmentMetrics(): F[Unit] = Async[F].unit
  override def updateDatabaseMetrics(databases: List[DatabaseInfo]): F[Unit] = Async[F].unit
  override def updateTableMetrics(databases: List[DatabaseInfo]): F[Unit] = Async[F].unit
  override def updateSegmentMetrics(databases: List[DatabaseInfo]): F[Unit] = Async[F].unit
}

object MetricsAdapter {
  
  /** Create Prometheus metrics adapter */
  def createPrometheusAdapter[F[_]: Async: Logger](
    collectorRegistry: io.prometheus.client.CollectorRegistry
  ): MetricsAdapter[F] = {
    new PrometheusMetricsAdapter(collectorRegistry)
  }
  
  /** Create no-op adapter for testing */
  def createNoOpAdapter[F[_]: Async]: MetricsAdapter[F] = {
    new NoOpMetricsAdapter()
  }
}
