package org.nazaroid.kvdb.statistics

import cats.effect.Async
import cats.implicits.given
import org.nazaroid.kvdb.core.CatalogStats
import org.typelevel.log4cats.Logger

/**
 * MetricsAdapter implementation for Bitcask engine with Prometheus
 */
trait MetricsAdapter[F[_]] {
  def registerMetrics(): F[Unit]
  def updateMetrics(stats: CatalogStats): F[Unit]
}
