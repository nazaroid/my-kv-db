package org.nazaroid.kvdb.engine

import org.nazaroid.kvdb.algebra.{Database, DbEngine}

final class BitcaskDbEngine[F[_]] extends DbEngine[F] {

  override def createDatabase(name: String): F[Database[F]] = ???
}
