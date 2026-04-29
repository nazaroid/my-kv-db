package org.nazaroid.kvdb.bitcask

import cats.effect.Async
import cats.implicits.given
import io.prometheus.client.{Counter, Histogram, Summary}
import org.nazaroid.kvdb.core.{CatalogStats, MetricsAdapter}
import org.typelevel.log4cats.Logger

/** Prometheus metrics adapter for Bitcask engine operations and statistics */
class PrometheusMetricsAdapter[F[_]: Async: Logger] extends MetricsAdapter[F] {

  // Operation counters
  private val getCounter: Counter = Counter
    .build()
    .name("bitcask_operations_get_total")
    .help("Total number of GET operations")
    .register()

  private val setCounter: Counter = Counter
    .build()
    .name("bitcask_operations_set_total")
    .help("Total number of SET operations")
    .register()

  private val deleteCounter: Counter = Counter
    .build()
    .name("bitcask_operations_delete_total")
    .help("Total number of DELETE operations")
    .register()

  // Operation histograms for timing
  private val getHistogram: Histogram = Histogram
    .build()
    .name("bitcask_operation_get_duration_seconds")
    .help("GET operation duration in seconds")
    .register()

  private val setHistogram: Histogram = Histogram
    .build()
    .name("bitcask_operation_set_duration_seconds")
    .help("SET operation duration in seconds")
    .register()

  private val deleteHistogram: Histogram = Histogram
    .build()
    .name("bitcask_operation_delete_duration_seconds")
    .help("DELETE operation duration in seconds")
    .register()

  // Operation summaries for additional statistics
  private val getSummary: Summary = Summary
    .build()
    .name("bitcask_operation_get_summary_seconds")
    .help("GET operation summary statistics in seconds")
    .register()

  private val setSummary: Summary = Summary
    .build()
    .name("bitcask_operation_set_summary_seconds")
    .help("SET operation summary statistics in seconds")
    .register()

  private val deleteSummary: Summary = Summary
    .build()
    .name("bitcask_operation_delete_summary_seconds")
    .help("DELETE operation summary statistics in seconds")
    .register()

  // Statistics gauges
  private val databasesGauge: Summary = Summary
    .build()
    .name("bitcask_databases_total")
    .help("Total number of databases")
    .register()

  private val tablesGauge: Summary = Summary
    .build()
    .name("bitcask_tables_total")
    .help("Total number of tables")
    .register()

  private val entriesGauge: Summary = Summary
    .build()
    .name("bitcask_entries_total")
    .help("Total number of entries")
    .register()

  private val activeEntriesGauge: Summary = Summary
    .build()
    .name("bitcask_entries_active_total")
    .help("Total number of active entries")
    .register()

  private val deletedEntriesGauge: Summary = Summary
    .build()
    .name("bitcask_entries_deleted_total")
    .help("Total number of deleted entries")
    .register()

  private val dataSizeGauge: Summary = Summary
    .build()
    .name("bitcask_data_size_bytes")
    .help("Total data size in bytes")
    .register()

  override def registerMetrics(): F[Unit] = {
    Logger[F].info("Registering Prometheus metrics for Bitcask engine")
  }

  override def updateMetrics(stats: CatalogStats): F[Unit] = {
    for {
      _ <- Logger[F].debug("Updating Prometheus metrics with catalog stats")
      _ <- Async[F].delay {
        databasesGauge.observe(stats.totalDatabases.toDouble)
        tablesGauge.observe(stats.totalTables.toDouble)
        entriesGauge.observe(stats.totalEntries.toDouble)
        activeEntriesGauge.observe(stats.activeEntries.toDouble)
        deletedEntriesGauge.observe(stats.deletedEntries.toDouble)
        dataSizeGauge.observe(stats.totalDataSize.toDouble)
      }
    } yield ()
  }

  // Methods to record operation metrics
  def recordGetOperation(durationSeconds: Double): F[Unit] = {
    Async[F].delay {
      getCounter.inc()
      getHistogram.observe(durationSeconds)
      getSummary.observe(durationSeconds)
    }
  }

  def recordSetOperation(durationSeconds: Double): F[Unit] = {
    Async[F].delay {
      setCounter.inc()
      setHistogram.observe(durationSeconds)
      setSummary.observe(durationSeconds)
    }
  }

  def recordDeleteOperation(durationSeconds: Double): F[Unit] = {
    Async[F].delay {
      deleteCounter.inc()
      deleteHistogram.observe(durationSeconds)
      deleteSummary.observe(durationSeconds)
    }
  }
}

object PrometheusMetricsAdapter {
  def create[F[_]: Async: Logger](): F[PrometheusMetricsAdapter[F]] = {
    Async[F].delay(new PrometheusMetricsAdapter[F]())
  }
}
