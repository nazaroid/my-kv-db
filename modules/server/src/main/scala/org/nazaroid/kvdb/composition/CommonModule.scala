package com.uzumdata.cc.api.composition

import cats.effect.Async
import cats.effect.std.Dispatcher
import com.uzumdata.cc.api.{AppConfig, AppMetrics}

final class CommonModule[F[_]: Async](
  c: AppConfig,
  s: AppState[F],
  m: AppMetrics,
  d: Dispatcher[F]) {
  implicit val appConfig:  AppConfig     = c
  implicit val appState:   AppState[F]   = s
  implicit val appMetrics: AppMetrics    = m
  implicit val dispatcher: Dispatcher[F] = d
}
