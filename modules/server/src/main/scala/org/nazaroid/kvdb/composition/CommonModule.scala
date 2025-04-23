package org.nazaroid.kvdb.composition

import cats.effect.Async
import cats.effect.std.Dispatcher
import org.nazaroid.kvdb.AppConfig

final class CommonModule[F[_]: Async](
  c: AppConfig,
  s: AppState[F],
  d: Dispatcher[F]) {
  implicit val appConfig:  AppConfig     = c
  implicit val appState:   AppState[F]   = s
  implicit val dispatcher: Dispatcher[F] = d
}
