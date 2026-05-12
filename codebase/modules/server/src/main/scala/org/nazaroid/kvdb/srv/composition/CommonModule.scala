package org.nazaroid.kvdb.srv.composition

import cats.effect.Async
import cats.effect.std.Dispatcher
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.srv.DbInstanceConfig

final class CommonModule[F[_]: Async](
  c: DbInstanceConfig,
  d: Dispatcher[F]) {
  implicit val dispatcher: Dispatcher[F]     = d
  val config:     DbInstanceConfig  = c
  val metricRegistry:   CollectorRegistry = CollectorRegistry.defaultRegistry
}
