package org.nazaroid.kvdb.srv.composition

import cats.effect.Async
import cats.effect.std.Dispatcher
import org.nazaroid.kvdb.DbConf
import org.nazaroid.kvdb.srv.DbSrvState

final class CommonModule[F[_]: Async](
                                       c: DbConf,
                                       s: DbSrvState[F],
                                       d: Dispatcher[F]) {
  implicit val config:     DbConf     = c
  implicit val state:      DbSrvState[F] = s
  implicit val dispatcher: Dispatcher[F] = d
}
