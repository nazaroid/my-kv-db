package org.nazaroid.kvdb.bitcask

import cats.effect.Async
import cats.implicits.given
import io.circe.*
import io.prometheus.client.*
import org.nazaroid.kvdb.bitcask.lib.BitcaskDatabaseStats
import org.nazaroid.kvdb.core.{CatalogStats, MetricsAdapter}
import org.typelevel.log4cats.Logger

/** Prometheus-based MetricsAdapter for Bitcask
  */
class BitcaskPrometheusMetricsAdapter[F[_]: Async: Logger](reg: CollectorRegistry) extends MetricsAdapter[F] {

  // Catalog-level metrics
  private val totalDatabasesGauge = Gauge
    .build()
    .name("bitcask_databases_total")
    .help("Total number of databases")
    .register(reg)

  private val totalTablesGauge = Gauge
    .build()
    .name("bitcask_tables_total")
    .help("Total number of tables across all databases")
    .register(reg)

  private val totalEntriesGauge = Gauge
    .build()
    .name("bitcask_entries_total")
    .help("Total number of entries across all databases")
    .register(reg)

  private val activeEntriesGauge = Gauge
    .build()
    .name("bitcask_entries_active_total")
    .help("Total number of active entries across all databases")
    .register(reg)

  private val deletedEntriesGauge = Gauge
    .build()
    .name("bitcask_entries_deleted_total")
    .help("Total number of deleted entries across all databases")
    .register(reg)

  private val totalDataSizeGauge = Gauge
    .build()
    .name("bitcask_data_size_bytes")
    .help("Total data size in bytes across all databases")
    .register(reg)

  private val totalSegmentsGauge = Gauge
    .build()
    .name("bitcask_segments_total")
    .help("Total number of segments across all databases")
    .register(reg)

  private val activeSegmentsGauge = Gauge
    .build()
    .name("bitcask_segments_active_total")
    .help("Total number of active segments across all databases")
    .register(reg)

  // Database-level metrics
  private val databaseEntriesGauge = Gauge
    .build()
    .name("bitcask_database_entries_total")
    .help("Total number of entries in a database")
    .labelNames("database")
    .register(reg)

  private val databaseTablesGauge = Gauge
    .build()
    .name("bitcask_database_tables_total")
    .help("Total number of tables in a database")
    .labelNames("database")
    .register(reg)

  private val databaseDataSizeGauge = Gauge
    .build()
    .name("bitcask_database_data_size_bytes")
    .help("Total data size in bytes for a database")
    .labelNames("database")
    .register(reg)

  // Table-level metrics
  private val tableEntriesGauge = Gauge
    .build()
    .name("bitcask_table_entries_total")
    .help("Total number of entries in a table")
    .labelNames("database", "table")
    .register(reg)

  private val tableActiveEntriesGauge = Gauge
    .build()
    .name("bitcask_table_entries_active_total")
    .help("Total number of active entries in a table")
    .labelNames("database", "table")
    .register(reg)

  private val tableDeletedEntriesGauge = Gauge
    .build()
    .name("bitcask_table_entries_deleted_total")
    .help("Total number of deleted entries in a table")
    .labelNames("database", "table")
    .register(reg)

  private val tableDataSizeGauge = Gauge
    .build()
    .name("bitcask_table_data_size_bytes")
    .help("Total data size in bytes for a table")
    .labelNames("database", "table")
    .register(reg)

  private val tableSegmentsGauge = Gauge
    .build()
    .name("bitcask_table_segments_total")
    .help("Total number of segments for a table")
    .labelNames("database", "table")
    .register(reg)

  // Segment-level metrics
  private val segmentSizeGauge = Gauge
    .build()
    .name("bitcask_segment_size_bytes")
    .help("Size of a segment in bytes")
    .labelNames("database", "table", "segment")
    .register(reg)

  private val segmentEntriesGauge = Gauge
    .build()
    .name("bitcask_segment_entries_total")
    .help("Number of entries in a segment")
    .labelNames("database", "table", "segment")
    .register(reg)

  private val segmentStaleRatioGauge = Gauge
    .build()
    .name("bitcask_segment_stale_data_ratio")
    .help("Ratio of stale data in a segment")
    .labelNames("database", "table", "segment")
    .register(reg)

  // Operation metrics
  private val writeOperationsCounter = Counter
    .build()
    .name("bitcask_write_operations_total")
    .help("Total number of write operations")
    .labelNames("database", "table", "status")
    .register(reg)

  private val writeOperationDuration = Histogram
    .build()
    .name("bitcask_write_operation_duration_seconds")
    .help("Duration of write operations in seconds")
    .labelNames("database", "table")
    .register(reg)

  private val readOperationsCounter = Counter
    .build()
    .name("bitcask_read_operations_total")
    .help("Total number of read operations")
    .labelNames("database", "table", "status")
    .register(reg)

  private val readOperationDuration = Histogram
    .build()
    .name("bitcask_read_operation_duration_seconds")
    .help("Duration of read operations in seconds")
    .labelNames("database", "table")
    .register(reg)

  private val deleteOperationsCounter = Counter
    .build()
    .name("bitcask_delete_operations_total")
    .help("Total number of delete operations")
    .labelNames("database", "table")
    .register(reg)

  private val deleteOperationDuration = Histogram
    .build()
    .name("bitcask_delete_operation_duration_seconds")
    .help("Duration of delete operations in seconds")
    .labelNames("database", "table")
    .register(reg)

  override def registerMetrics(): F[Unit] = {
    Logger[F].info("Bitcask Prometheus metrics registered")
  }

  override def updateMetrics(stats: CatalogStats): F[Unit] = {
    for {
      _ <- Logger[F].debug(
        s"Updating metrics from CatalogStats: ${stats.totalDatabases} databases, ${stats.totalTables} tables"
      )

      // Update catalog-level metrics
      _ <- Async[F].delay {
        totalDatabasesGauge.set(stats.totalDatabases.toDouble)
        totalTablesGauge.set(stats.totalTables.toDouble)
        totalEntriesGauge.set(stats.totalEntries.toDouble)
        activeEntriesGauge.set(stats.activeEntries.toDouble)
        deletedEntriesGauge.set(stats.deletedEntries.toDouble)
        totalDataSizeGauge.set(stats.totalDataSize.toDouble)

        // Extract segment information from details
        val totalSegments = stats.details.get("total_segments").flatMap(_.asNumber).map(_.toDouble).getOrElse(0.0)
        val activeSegments = stats.details.get("active_segments").flatMap(_.asNumber).map(_.toDouble).getOrElse(0.0)

        totalSegmentsGauge.set(totalSegments)
        activeSegmentsGauge.set(activeSegments)
      }

      // Extract database and table information from details
      databasesInfo = extractDatabasesFromDetails(stats.details)

      // Update database-level metrics
      _ <- databasesInfo.traverse_ { dbStats =>
        Async[F].delay {
          databaseEntriesGauge.labels(dbStats.name).set(dbStats.totalEntries.toDouble)
          databaseTablesGauge.labels(dbStats.name).set(dbStats.totalTables.toDouble)
          databaseDataSizeGauge.labels(dbStats.name).set(dbStats.totalDataSize.toDouble)
        }
      }

      // Update table-level metrics
      _ <- databasesInfo.traverse_ { dbStats =>
        dbStats.tableStats.traverse_ { tableStats =>
          Async[F].delay {
            tableEntriesGauge.labels(dbStats.name, tableStats.name).set(tableStats.totalEntries.toDouble)
            tableActiveEntriesGauge.labels(dbStats.name, tableStats.name).set(tableStats.activeEntries.toDouble)
            tableDeletedEntriesGauge.labels(dbStats.name, tableStats.name).set(tableStats.deletedEntries.toDouble)
            tableDataSizeGauge.labels(dbStats.name, tableStats.name).set(tableStats.totalDataSize.toDouble)
            tableSegmentsGauge.labels(dbStats.name, tableStats.name).set(tableStats.segmentCount.toDouble)
          }
        }
      }

      // Update segment-level metrics
      _ <- databasesInfo.traverse_ { dbStats =>
        dbStats.tableStats.traverse_ { tableStats =>
          tableStats.segments.traverse_ { segmentStats =>
            Async[F].delay {
              segmentSizeGauge
                .labels(dbStats.name, tableStats.name, segmentStats.name)
                .set(segmentStats.fileSize.toDouble)
              segmentEntriesGauge
                .labels(dbStats.name, tableStats.name, segmentStats.name)
                .set(segmentStats.entryCount.toDouble)
              segmentStaleRatioGauge
                .labels(dbStats.name, tableStats.name, segmentStats.name)
                .set(segmentStats.staleDataRatio)
            }
          }
        }
      }

    } yield ()
  }

  /** Extract database information from CatalogStats details */
  private def extractDatabasesFromDetails(details: Map[String, Json]): List[BitcaskDatabaseStats] = {
    details.get("databases") match {
      case Some(databasesJson) =>
        databasesJson.as[List[BitcaskDatabaseStats]] match {
          case Right(databases) => databases
          case Left(_)          => List.empty
        }
      case None => List.empty
    }
  }

  // Operation metrics recording methods
  def recordWriteOperation(database: String, table: String, success: Boolean, duration: Double): F[Unit] = {
    Async[F].delay {
      val status = if (success) "success" else "failure"
      writeOperationsCounter.labels(database, table, status).inc()
      writeOperationDuration.labels(database, table).observe(duration)
    }
  }

  def recordReadOperation(database: String, table: String, found: Boolean, duration: Double): F[Unit] = {
    Async[F].delay {
      val status = if (found) "hit" else "miss"
      readOperationsCounter.labels(database, table, status).inc()
      readOperationDuration.labels(database, table).observe(duration)
    }
  }

  def recordDeleteOperation(database: String, table: String, duration: Double): F[Unit] = {
    Async[F].delay {
      deleteOperationsCounter.labels(database, table).inc()
      deleteOperationDuration.labels(database, table).observe(duration)
    }
  }

  // Convenience methods for operations without explicit database/table names
  def recordWriteOperation(success: Boolean, duration: Double): F[Unit] = {
    recordWriteOperation("unknown", "unknown", success, duration)
  }

  def recordReadOperation(found: Boolean, duration: Double): F[Unit] = {
    recordReadOperation("unknown", "unknown", found, duration)
  }

  def recordDeleteOperation(duration: Double): F[Unit] = {
    recordDeleteOperation("unknown", "unknown", duration)
  }
}

object BitcaskPrometheusMetricsAdapter {

  def create[F[_]: Async: Logger](reg: CollectorRegistry): F[BitcaskPrometheusMetricsAdapter[F]] = {
    for {
      _ <- Logger[F].info("Creating Bitcask Prometheus metrics adapter")
      adapter = new BitcaskPrometheusMetricsAdapter[F](reg: CollectorRegistry)
    } yield adapter
  }
}
