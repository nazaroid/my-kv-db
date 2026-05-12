package org.nazaroid.kvdb.core

trait CatalogMetricRecorder[F[_]] {
  def recordMetrics(stats: CatalogStats): F[Unit]
}

trait PerformanceMetricRecorder[F[_]] {
  def recordGetOperation(durationSeconds: Double): F[Unit]

  def recordSetOperation(durationSeconds: Double): F[Unit]

  def recordDeleteOperation(durationSeconds: Double): F[Unit]
}
