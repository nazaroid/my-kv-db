package org.nazaroid.kvdb.bitcask

import cats.effect.Async
import cats.implicits.given
import io.prometheus.client.{CollectorRegistry, Counter, Histogram, Summary}
import org.nazaroid.kvdb.core.PerformanceMetricRecorder
import org.typelevel.log4cats.Logger

sealed class BitcaskPerformanceMetricRecorder[F[_]: Async: Logger](reg: CollectorRegistry)
    extends PerformanceMetricRecorder[F] {

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

object BitcaskPerformanceMetricRecorder {

  def create[F[_]: Async: Logger](reg: CollectorRegistry): F[PerformanceMetricRecorder[F]] = {
    Logger[F].info("сreating Bitcask Performance metric recorder") >> Async[F].delay(
      new BitcaskPerformanceMetricRecorder[F](reg)
    )
  }
}
