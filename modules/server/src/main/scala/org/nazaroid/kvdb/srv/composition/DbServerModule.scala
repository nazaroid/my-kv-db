package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.effect.Async
import org.nazaroid.kvdb.algebra.DbServer
import org.nazaroid.kvdb.srv.http.HttpDbServer
import org.typelevel.log4cats.Logger

final class DbServerModule[F[_]: Async: Logger: Parallel](commonModule: CommonModule[F]) {
  import commonModule.*

  def resolve: F[DbServer[F]] = {
    new HttpDbServer[F](config)
  }

}
