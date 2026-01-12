package org.nazaroid.kvdb.bitcask.lib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async

import java.nio.charset.StandardCharsets
import java.nio.file.Paths

trait LibScenarios[F[_]: Async] {
  given env: Env[F]

  def readDbCatalog(): F[DbCatalog] = ???

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

    def readTblIxFile(filePath: String): List[(Key, SegmentName)] = {
      import java.io.*
      import java.nio.ByteBuffer
      import scala.collection.mutable.ListBuffer

      val file = new File(filePath)

      val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
      val records = ListBuffer[(Key, SegmentName)]()
      try {
        while (true) {
          val recordSize = ByteBuffer.wrap(dis.readNBytes(4)).getInt()
          val keySize = ByteBuffer.wrap(dis.readNBytes(4)).getInt()
          val keyBytes = dis.readNBytes(keySize)
          val segmentNameSize = ByteBuffer.wrap(dis.readNBytes(4)).getInt()
          val segmentNameBytes = dis.readNBytes(segmentNameSize)
          val key: Key = String(keyBytes, StandardCharsets.UTF_8)
          val segmentName: SegmentName = String(segmentNameBytes, StandardCharsets.UTF_8)
          val record: (Key, SegmentName) = (key, segmentName)
          records += record
        }
      } catch {
        case _: EOFException =>
      }
      records.toList
    }

    def loadTblIxData(filePath: String, segments: Map[SegmentName, Segment[F]]): DbScript[F, TblIxData[F]] = {
      for {
        env <- ask[F, Env[F]]
        fileData = readTblIxFile(filePath)
        tblIxData = fileData.map((k, segName) => (k, segments(segName))).toMap
      } yield tblIxData
    }

    def readSegmentIxFile(filePath: String): List[(Key, Offset)] = {
      import java.io.*
      import java.nio.ByteBuffer
      import scala.collection.mutable.ListBuffer

      val file = new File(filePath)

      val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
      val records = ListBuffer[(Key, Offset)]()
      try {
        while (true) {
          val recordSize = ByteBuffer.wrap(dis.readNBytes(4)).getInt()
          val keySize = ByteBuffer.wrap(dis.readNBytes(4)).getInt()
          val keyBytes = dis.readNBytes(keySize)
          val offsetSize = 8
          val key: Key = String(keyBytes, StandardCharsets.UTF_8)
          val offset = ByteBuffer.wrap(dis.readNBytes(offsetSize)).getLong()
          val record: (Key, Offset) = (key, offset)
          records += record
        }
      } catch {
        case _: EOFException =>
      }
      records.toList
    }

    def loadSegmentIxData(filePath: String): DbScript[F, SegmentIxData] = {
      for {
        env <- ask[F, Env[F]]
        fileData = readSegmentIxFile(filePath)
        segmentIxData = fileData.toMap
      } yield segmentIxData
    }

    def loadSegment(filePath: String): DbScript[F, Segment[F]] = {
      val path = Paths.get(filePath)
      val segmentName: SegmentName = path.getFileName.toString
      val offset: Offset = path.toFile.length()
      val segmentNum: SegmentNum = segmentName.substring(segmentName.indexOf("_"), segmentName.indexOf(".")).toInt
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
