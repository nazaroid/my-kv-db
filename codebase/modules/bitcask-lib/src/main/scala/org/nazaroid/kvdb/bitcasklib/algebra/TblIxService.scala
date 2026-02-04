package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async
import fs2.io.file.Files
import org.nazaroid.kvdb.binfileio.*

trait TblIxService[F[_]: Async: Files] {

  private val schema = List(
    FieldDef("keySize", FieldType.Int32),
    FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
    FieldDef("segmentNameSize", FieldType.Int32),
    FieldDef("segmentName", FieldType.StringUtf8(sizeFromField = "segmentNameSize"))
  )

  def create(tbl: Tbl[F]): DbScript[F, TblIx[F]] = {
    for {
      env <- ask[F, Env[F]]
      ix  <- DbScript.lift(TblIx.create(tbl))
      _   <- env.files.createFile(ix.path)
      _   <- env.cache.addTblIx(tbl, ix)
    } yield ix
  }

  def update(
    ix:  TblIx[F],
    key: Key,
    s:   Segment[F]
  ): DbScript[F, TblIx[F]] = {
    for {
      env <- ask[F, Env[F]]
      row: Row = Map("key" -> key, "segmentName" -> s.name, "keySize" -> key.length, "segmentNameSize" -> s.name.length)
      _ <- env.files.appendToFile(ix.path, schema, row)
      _ <- env.cache.updateTblIx(ix, key, s)
    } yield ix
  }

  def readData(filePath: String, segments: Map[SegmentName, Segment[F]]): DbScript[F, TblIxData[F]] = {
    def readTblIxFile(filePath: String): fs2.Stream[F, (Key, SegmentName)] = {
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
}
