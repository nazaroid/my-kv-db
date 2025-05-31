package org.nazaroid.kvdb.engine

import cats.effect.Async
import cats.implicits.*
import org.nazaroid.kvdb.algebra.{Database, DbEngine, Table}
import org.nazaroid.kvdb.engine.BitcaskDbEngine.*

final class BitcaskDbEngine[F[_]: Async] extends DbEngine[F] {

  private var dbs: Map[String, StubDatabase[F]] = Map.empty[String, StubDatabase[F]]

  override def createDatabase(name: String): F[Unit] = {
    dbs = dbs.updated(name, dbs.getOrElse(name, new StubDatabase[F]()))
    ().pure[F]
  }

  override def getDatabase(name: String): F[Database[F]] = {
    dbs(name).pure[F]
  }
}

object BitcaskDbEngine {

  private class StubDatabase[F[_]: Async] extends Database[F] {
    private var tbls: Map[String, Table[F]] = Map.empty[String, Table[F]]

    override def createTable(name: String): F[Unit] = {
      tbls = tbls.updated(name, tbls.getOrElse(name, new StubTable[F]()))
      ().pure[F]
    }

    override def getTable(name: String): F[Table[F]] = {
      tbls(name).pure[F]
    }
  }

  private class StubTable[F[_]: Async] extends Table[F] {
    private var vals: Map[String, String] = Map.empty[String, String]

    override def get(key: String): F[String] = vals(key).pure[F]

    override def set(key: String, value: String): F[Unit] = {
      vals = vals + (key -> value)
      ().pure[F]
    }
  }
}
