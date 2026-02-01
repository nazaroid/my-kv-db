package org.nazaroid.kvdb.engine.bitcask

import cats.effect.Async
import cats.implicits.*
import org.nazaroid.kvdb.algebra.DbEngine
import org.nazaroid.kvdb.bitcasklib.BitcaskConf
import org.nazaroid.kvdb.bitcasklib.algebra.*

final class BitcaskDbEngine[F[_]: Async](conf: BitcaskConf, lib: LibScenarios[F]) extends DbEngine[F] {

  override def init(): F[Unit] = {
    lib.readDbCatalog() >>= lib.init
  }

  override def createDbIfNotExists(name: String): F[Unit] = {
    lib.createBaseIfNotExists(name) >> ().pure[F]
  }

  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    lib.createTableIfNotExists(baseName, tblName) >> ().pure[F]
  }

  override def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = lib.get(baseName, tblName, key)

  override def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    lib.set(baseName, tblName, key, value)
  }

}
