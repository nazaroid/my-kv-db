package org.nazaroid.kvdb.bitcask.lib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async

trait LibScenarios[F[_]: Async] {
  given env: Env[F]

  def readDbCatalog(): F[DbCatalog] = ???

  def init(dbCatalog: DbCatalog): F[Unit] = {
    ???
  }

  def createBaseIfNotExists(baseName: String): F[Base[F]] =
    DbScript.run {
      for {
        env  <- ask[F, Env[F]]
        base <- env.base.createIfNotExists(env.conf.rootDir, baseName)
      } yield base
    }

  def createTableIfNotExists(baseName: String, tblName: String): F[Tbl[F]] =
    DbScript.run {
      for {
        env  <- ask[F, Env[F]]
        base <- env.base.get(baseName)
        tbl  <- env.tbl.createIfNotExists(base, tblName)
      } yield tbl
    }

  def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = {
    DbScript.run {
      for {
        env  <- ask[F, Env[F]]
        base <- env.base.get(baseName)
        tbl  <- env.tbl.get(base, tblName)
        vOpt <- env.tbl.findInSegments(tbl, key)
      } yield vOpt
    }
  }

  def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    DbScript.run {
      for {
        env  <- ask[F, Env[F]]
        base <- env.base.get(baseName)
        tbl  <- env.tbl.get(base, tblName)
        _    <- env.tbl.appendToLastSegment(tbl, key, value)
      } yield ()
    }
  }
}
