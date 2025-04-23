package org.nazaroid.kvdb.composition

import cats.effect.{Async, Ref}

final case class AppState[F[_]: Async](
  someRef: Ref[F, String])
