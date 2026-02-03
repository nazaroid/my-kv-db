package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.*
import fs2.io.file.Files
import org.nazaroid.kvdb.binfileio.*
import org.nazaroid.kvdb.bitcasklib.bindata.*

trait SegmentIxService[F[_]: Async: Files] {

  def create(s: Segment[F]): DbScript[F, SegmentIx[F]] = {
    for {
      env <- ask[F, Env[F]]
      ix  <- DbScript.lift(SegmentIx.create(s))
      _   <- env.files.createFile(ix.path)
      _   <- env.cache.addSegmentIx(s, ix)
    } yield ix
  }

  def update(
    ix:     SegmentIx[F],
    key:    Key,
    offset: Offset
  ): DbScript[F, SegmentIx[F]] = {
    for {
      env    <- ask[F, Env[F]]
      record <- DbScript.lift(SegmentIxRecord.create(key, offset))
      _      <- env.files.appendToFile(ix.path, record)
      _      <- env.cache.updateSegmentIx(ix, key, offset)
    } yield ix
  }


  def readData(filePath: String): DbScript[F, SegmentIxData] = {
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

}
