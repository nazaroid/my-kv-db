package org.nazaroid.kvdb.utils.metrics

import cats.effect.Async
import cats.implicits.*
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory

final class MetricExporter[F[_]: Async](metricsPort: Int) {

  private val logger: SelfAwareStructuredLogger[F] = Slf4jFactory.create[F].getLogger

  def start(): F[Unit] = {
    for {
      _ <- logger.info(s"Metrics starting on port $metricsPort...")
      _ <- Async[F].delay(new HTTPServer(metricsPort))
      _ <- Async[F].delay(DefaultExports.initialize())
      _ <- logger.info(s"Metrics server ready!")
    } yield ()
  }
}
