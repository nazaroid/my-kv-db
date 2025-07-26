package org.nazaroid.kvdb.srv.composition

import cats.Parallel
import cats.effect.Async
import cats.implicits.given
import fs2.io.net.Network
import org.nazaroid.kvdb.algebra.{DbEngine, DbServer}
import org.nazaroid.kvdb.bitcask.lib.BitcaskLib
import org.nazaroid.kvdb.engine.bitcask.BitcaskDbEngine
import org.nazaroid.kvdb.srv.http.HttpDbServer
import org.typelevel.log4cats.Logger

final class DbServerModule[F[_]: Async: Logger: Parallel: Network](commonModule: CommonModule[F]) {
  import commonModule.*

  def resolve: F[DbServer[F]] = {
    for {
      libState <- BitcaskLib.createState()
      conf = config.engine.bitcask
      lib = BitcaskLib(conf, libState)
      engine: DbEngine[F] = new BitcaskDbEngine(conf, lib) // new StubDbEngine[F]()
    } yield {
      new HttpDbServer[F](config.server.http, engine)
    }
  }
}
