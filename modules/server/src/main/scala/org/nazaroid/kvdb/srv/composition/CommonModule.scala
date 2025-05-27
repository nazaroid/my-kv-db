package org.nazaroid.kvdb.srv.composition

import cats.effect.Async
import cats.effect.std.Dispatcher
import org.nazaroid.kvdb.srv.{DbSrvConf, DbSrvState}

final class CommonModule[F[_]: Async](
  c: DbSrvConf,
  s: DbSrvState[F],
  d: Dispatcher[F]) {
  implicit val config:     DbSrvConf     = c
  implicit val state:      DbSrvState[F] = s
  implicit val dispatcher: Dispatcher[F] = d
}
