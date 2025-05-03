package org.nazaroid.kvdb.srv

import cats.effect.{Async, Ref}

final case class DbSrvState[F[_]: Async](
  someRef: Ref[F, String])
