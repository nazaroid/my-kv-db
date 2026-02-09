package org.nazaroid.kvdb.srv.composition

import cats.effect.Async
import cats.effect.std.Dispatcher
import org.nazaroid.kvdb.DbConf

final class CommonModule[F[_]: Async](
  c: DbConf,
  d: Dispatcher[F]) {
  implicit val config:     DbConf        = c
  implicit val dispatcher: Dispatcher[F] = d
}
