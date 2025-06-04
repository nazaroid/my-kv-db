package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.effect.Async
import cats.implicits.given
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.{DbEngine, DbServer}
import org.nazaroid.kvdb.engine.stub.StubDbEngine
import org.nazaroid.kvdb.srv.http.HttpDbServer
import org.typelevel.log4cats.Logger

final class DbServerModule[F[_]: Async: Logger: Parallel: Network](commonModule: CommonModule[F]) {
  import commonModule.*

  def resolve: F[DbServer[F]] = {
    val engine: DbEngine[F] = new StubDbEngine[F]()
    new HttpDbServer[F](config, engine).pure[F]
  }

}
