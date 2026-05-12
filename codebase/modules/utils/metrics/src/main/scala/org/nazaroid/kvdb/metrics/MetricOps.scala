package org.nazaroid.kvdb.metrics

import cats.Monad
import cats.implicits.given

import scala.concurrent.duration.FiniteDuration

extension [F[_], A](fa: F[(FiniteDuration, A)])
  def recordTo(record: FiniteDuration => F[Unit])(using F: Monad[F]): F[A] =
    fa.flatMap { (duration, result) =>
      record(duration).as(result)
    }