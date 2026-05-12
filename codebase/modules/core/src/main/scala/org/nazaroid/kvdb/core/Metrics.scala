package org.nazaroid.kvdb.core

import scala.concurrent.duration.FiniteDuration

trait CatalogMetricRecorder[F[_]] {
  def recordMetrics(stats: CatalogStats): F[Unit]
}

trait PerformanceMetricRecorder[F[_]] {
  def recordGetOperation(duration: FiniteDuration): F[Unit]

  def recordSetOperation(duration: FiniteDuration): F[Unit]

  def recordDeleteOperation(duration: FiniteDuration): F[Unit]
}
