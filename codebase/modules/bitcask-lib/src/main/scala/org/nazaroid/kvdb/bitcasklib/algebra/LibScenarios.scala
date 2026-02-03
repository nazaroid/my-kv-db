package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.*
import cats.implicits.given
import cats.syntax.all.*
import fs2.io.file.Files
import org.nazaroid.kvdb.binfileio.*

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

    def loadTblIxData(filePath: String, segments: Map[SegmentName, Segment[F]]): DbScript[F, TblIxData[F]] = {
      def readTblIxFile(filePath: String): fs2.Stream[F, (Key, SegmentName)] = {
        val schema = List(
          FieldDef("recordSize", FieldType.Int32),
          FieldDef("keySize", FieldType.Int32),
          FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
          FieldDef("segmentNameSize", FieldType.Int32),
          FieldDef("segmentName", FieldType.StringUtf8(sizeFromField = "segmentNameSize"))
        )

        BinFileIO
          .read[F](filePath, schema)
          .map(r => (r("key").asInstanceOf[Key], r("segmentName").asInstanceOf[SegmentName]))
      }

      val stream = for {
        (key, segmentName) <- readTblIxFile(filePath)
      } yield (key, segments(segmentName))

      DbScript.lift(stream.compile.fold(Map.empty[Key, Segment[F]]) { case (acc, (k, s)) =>
        acc + (k -> s)
      })
    }

    def loadSegmentIxData(filePath: String): DbScript[F, SegmentIxData] = {
      def readSegmentIxFile(filePath: String): fs2.Stream[F, (Key, Offset)] = {
        val schema = List(
          FieldDef("recordSize", FieldType.Int32),
          FieldDef("keySize", FieldType.Int32),
          FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
          FieldDef("offset", FieldType.Int64)
        )

        BinFileIO
          .read[F](filePath, schema)
          .map(r => (r("key").asInstanceOf[Key], r("offset").asInstanceOf[Offset]))
      }

      DbScript.lift(readSegmentIxFile(filePath).compile.fold(Map.empty[Key, Offset]) { case (acc, (k, s)) =>
        acc + (k -> s)
      })
    }

    def loadSegment(filePath: String): DbScript[F, Segment[F]] = {
      val path = Paths.get(filePath)
      val segmentName: SegmentName = path.getFileName.toString
      val offset: Offset = path.toFile.length()
      val segmentNum: SegmentNum = segmentName.substring(segmentName.indexOf("_") + 1, segmentName.indexOf(".")).toInt
      val emptyIx = None.asInstanceOf[Option[SegmentIx[F]]]
      val isReadOnly = false
      for {
        offsetRef  <- DbScript.lift(Async[F].ref(offset))
        emptyIxRef <- DbScript.lift(Async[F].ref(emptyIx))
      } yield Segment(segmentNum, segmentName, path, emptyIxRef, offsetRef, isReadOnly)
    }

    DbScript.run {
      for {
        env <- ask[F, Env[F]]
        // TODO:
        // - impl load funs
        //   + loadTblIxData
        //   + loadSegment
        //   + loadSegmentIxData
        // + add to cache
        // refact: move paths to DbCatalog (bases *> tables[: tbl ix) *> segments[: seg_ix]
        // - impl load DataCatalog
        // func style

        // plan:
        // create segmentIx
        // create segment
        // create tblIx
        // create tbl
        // create base

        // segments
        segmentIxData    <- loadSegmentIxData(segmentIxPath)
        segmentIxDataRef <- DbScript.lift(Async[F].ref(segmentIxData))
        segmentIx = SegmentIx(segmentIxName, Paths.get(segmentIxPath), segmentIxDataRef)
        segment <- loadSegment(segmentPath)
        tblSegments = Map(segment.name -> segment)
        _ <- DbScript.lift(segment.ix.set(Some(segmentIx)))
        // tables
        tblIxData    <- loadTblIxData(tblIxPath, tblSegments)
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
