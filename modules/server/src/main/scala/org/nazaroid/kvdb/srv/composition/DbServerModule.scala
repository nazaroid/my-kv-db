package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.effect.Async
import cats.implicits.given
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.{DbRuntime, DbServer}
import org.nazaroid.kvdb.srv.http.HttpDbServer
import org.typelevel.log4cats.Logger

final class DbServerModule[F[_]: Async: Logger: Parallel: DbRuntime: Network](commonModule: CommonModule[F]) {
  import commonModule.*

  def resolve: F[DbServer[F]] = {
    new HttpDbServer[F](config).pure[F]
  }

}
