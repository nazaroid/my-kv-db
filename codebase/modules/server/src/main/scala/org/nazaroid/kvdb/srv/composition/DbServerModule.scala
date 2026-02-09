package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.effect.Async
import fs2.io.file.Files
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.DbServer
import org.nazaroid.kvdb.engine.bitcask.BitcaskDbEngine
import org.nazaroid.kvdb.srv.http.HttpDbServer
import org.typelevel.log4cats.Logger

final class DbServerModule[F[_]: Async: Files: Logger: Parallel: Network](commonModule: CommonModule[F]) {
  import commonModule.*

  def resolve: F[DbServer[F]] = {
    val engine = BitcaskDbEngine.init[F](config.engine.bitcask)
    Async[F].pure(new HttpDbServer[F](config.server.http, engine))
  }
}
