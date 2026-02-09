package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.effect.Async
import fs2.io.file.Files
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.Server
import org.nazaroid.kvdb.engine.BitcaskEngine
import org.nazaroid.kvdb.srv.http.HttpServer
import org.typelevel.log4cats.Logger

final class ServerModule[F[_]: Async: Files: Logger: Parallel: Network](commonModule: CommonModule[F]) {
  import commonModule.*

  def resolve: F[Server[F]] = {
    val engine = BitcaskEngine.init[F](config.engine)
    Async[F].pure(new HttpServer[F](config.server, engine))
  }
}
