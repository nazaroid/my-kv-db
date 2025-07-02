package org.nazaroid.kvdb.engine.stub

import cats.effect.Async
import cats.implicits.*
import org.nazaroid.kvdb.algebra.DbEngine
import org.nazaroid.kvdb.engine.stub.StubDbEngine.*

final class StubDbEngine[F[_]: Async] extends DbEngine[F] {

  private var dbs: Map[String, StubDatabase[F]] = Map.empty[String, StubDatabase[F]]

  override def createDbIfNotExists(name: String): F[Unit] = {
    dbs = dbs.updated(name, dbs.getOrElse(name, new StubDatabase[F]()))
    ().pure[F]
  }

  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    dbs(baseName).createTableIfNotExists(tblName) >> ().pure[F]
  }

  def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = {
    dbs(baseName).getTable(tblName).get(key)
  }

  def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    dbs(baseName).getTable(tblName).set(key, value)
  }
}

object StubDbEngine {

  private class StubDatabase[F[_]: Async] {
    private var tbls: Map[String, StubTable[F]] = Map.empty[String, StubTable[F]]

    def createTableIfNotExists(name: String): F[StubTable[F]] = {
      tbls = tbls.updated(name, tbls.getOrElse(name, new StubTable[F]()))
      tbls(name).pure[F]
    }

    def getTable(name: String): StubTable[F] = tbls(name)

  }

  private class StubTable[F[_]: Async] {
    private var vals: Map[String, String] = Map.empty[String, String]

    def get(key: String): F[Option[String]] = vals.get(key).pure[F]

    def set(key: String, value: String): F[Unit] = {
      vals = vals + (key -> value)
      ().pure[F]
    }
  }
}
