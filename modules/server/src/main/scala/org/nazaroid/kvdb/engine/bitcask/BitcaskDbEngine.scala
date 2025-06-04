package org.nazaroid.kvdb.engine.bitcask

import cats.effect.Async
import org.nazaroid.kvdb.algebra.{Database, DbEngine, Table}

final class BitcaskDbEngine[F[_]: Async] extends DbEngine[F] {

  override def createDbIfNotExists(name: String): F[Database[F]] = {
    ???
  }
}

object BitcaskDbEngine {

  private class BitcaskDatabase[F[_]: Async] extends Database[F] {

    override def createTableIfNotExists(name: String): F[Table[F]] = {
      // TODO:
      ???
    }
  }

  private class BitcaskTable[F[_]: Async] extends Table[F] {
    override def get(key: String): F[String] = ???

    override def set(key: String, value: String): F[Unit] = {
      ???
    }
  }
}
