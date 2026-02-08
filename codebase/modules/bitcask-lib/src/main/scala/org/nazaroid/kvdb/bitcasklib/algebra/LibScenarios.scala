package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.*
import cats.implicits.given
import fs2.io.file.Files

// TODO: refact

/*
выделить сущности: Catalog, Database, Table

снуружи
for {
  engine <- BitcaskEngine.init[IO](config)

   _ <- engine.crateDbIfNotExists
   _ <- engine.crateTableIfNotExists
   _ <- engine.get
   _ <- engine.set

} yield ()

внутри

for {
  // 1. Инициализируем корень
  catalog <-Catalog.init[IO](Path("./my_storage"))

  // 2. Выбираем базу данных
  db <- catalog.database("prod_db")

  // 3. Выбираем таблицу
  users <- db.table("users")

  // 4. Работаем с данными
  _ <- users.write("user:100", "Alice")
  data <- users.read("user:100")

  // 5. Обслуживание (например, для всех таблиц в базе)
  _ <- db.listTables().evalMap(name => db.table(name).flatMap(_.compact())).compile.drain

} yield ()
*/
trait LibScenarios[F[_]: Async: Files] {
  given env: Env[F]

  def readDbCatalog(): F[DbCatalog] = {
    DbCatalog().pure[F]
  }

  def init(dbCatalog: DbCatalog): F[Unit] = {
    val rootDir = "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/kvdb/"
    val baseName = "db"
    val basePath = "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/kvdb/db"

    val tblName = "tbl"
    val tblPath = "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/kvdb/db/tbl"

    val tblIxName = "table.ix"
    val tblIxPath = "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/kvdb/db/tbl/table.ix"

    val segmentName = "segment_1.seg"
    val segmentPath =
      "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/kvdb/db/tbl/segment_1.seg"

    val segmentIxName = "segment_1.ix.ix"
    val segmentIxPath =
      "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/kvdb/db/tbl/segment_1.ix"

    DbScript.run {
      for {
        env <- ask[F, Env[F]]
        _   <- env.files.initFileService()
        // TODO:
        // - impl load funs
        //   + loadTblIxData
        //   + loadSegment
        //   + loadSegmentIxData
        // + add to cache
        // refact: move paths to DbCatalog (bases *> tables[: tbl ix) *> segments[: seg_ix]
        // - impl load DataCatalog
        // func style
        // move от bindata

        base <- env.base.createIfNotExists(rootDir, baseName)
        _    <- env.tbl.createIfNotExists(base, tblName)
      } yield ()
    }
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
  ): F[Option[Value]] = {
    DbScript.run {
      for {
        env  <- ask[F, Env[F]]
        base <- env.base.get(baseName)
        tbl  <- env.tbl.get(base, tblName)
        vOpt <- env.tbl.readValue(tbl, key)
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
        _    <- env.tbl.writeValue(tbl, key, value)
      } yield ()
    }
  }
}
