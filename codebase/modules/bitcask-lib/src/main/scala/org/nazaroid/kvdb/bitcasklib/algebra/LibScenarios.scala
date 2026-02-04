package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.*
import cats.implicits.given
import fs2.io.file.Files

import java.nio.file.Paths

trait LibScenarios[F[_]: Async: Files] {
  given env: Env[F]

  def readDbCatalog(): F[DbCatalog] = {
    DbCatalog().pure[F]
  }

  def init(dbCatalog: DbCatalog): F[Unit] = {
    val rootDir = "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/codebase/modules/server/kvdb/"
    val baseName = "db"
    val basePath = "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/codebase/modules/server/kvdb/db"

    val tblName = "tbl"
    val tblPath = "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/codebase/modules/server/kvdb/db/tbl"

    val tblIxName = "table.ix"
    val tblIxPath = "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/codebase/modules/server/kvdb/db/tbl/table.ix"

    val segmentName = "segment_1.seg"
    val segmentPath =
      "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/codebase/modules/server/kvdb/db/tbl/segment_1.seg"

    val segmentIxName = "segment_1.ix.ix"
    val segmentIxPath =
      "/Users/artem.nazarenko/IdeaProjects/my/my-kv-db/codebase/modules/server/kvdb/db/tbl/segment_1.ix"

    DbScript.run {
      for {
        env    <- ask[F, Env[F]]
        _      <- env.files.initFileService()
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

        // plan:
        // create segmentIx
        // create segment
        // create tblIx
        // create tbl
        // create base

        // segments
        segmentIxData    <- env.segmentIx.readData(segmentIxPath)
        segmentIxDataRef <- DbScript.lift(Async[F].ref(segmentIxData))
        segmentIx = SegmentIx(segmentIxName, Paths.get(segmentIxPath), segmentIxDataRef)
        segment <- env.segment.read(segmentPath)
        tblSegments = Map(segment.name -> segment)
        _ <- DbScript.lift(segment.ix.set(Some(segmentIx)))
        // tables
        tblIxData    <- env.tblIx.readData(tblIxPath, tblSegments)
        tblIxDataRef <- DbScript.lift(Async[F].ref(tblIxData))
        tblIx = TblIx[F](tblIxName, Paths.get(tblIxPath), tblIxDataRef)
        tblIxRef       <- DbScript.lift(Async[F].ref(Option(tblIx)))
        lastSegmentRef <- DbScript.lift(Async[F].ref(Option(segment)))
        tbl = Tbl[F](tblName, Paths.get(tblPath), lastSegmentRef, tblIxRef)
        baseTbls <- DbScript.lift(Async[F].ref(Map(tblName -> tbl)))
        // base
        base = Base(baseName, Paths.get(basePath), baseTbls)

        _ <- env.cache.addBase(base)
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
