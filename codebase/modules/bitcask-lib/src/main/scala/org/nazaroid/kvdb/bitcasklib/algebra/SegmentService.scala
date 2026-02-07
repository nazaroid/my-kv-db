package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.*
import cats.implicits.given
import cats.syntax.all.*
import fs2.io.file.Files
import org.nazaroid.kvdb.binfileio.*
import org.nazaroid.kvdb.bitcasklib.bindata.*

import java.nio.file.Paths

trait SegmentService[F[_]: Async: Files] {

  def create(tbl: Tbl[F], num: SegmentNum): DbScript[F, Segment[F]] = {
    for {
      env <- ask[F, Env[F]]
      s   <- DbScript.lift(Segment.create(tbl))
      _   <- env.files.createFile(s.path)
      _   <- env.cache.addSegment(tbl, s)
    } yield s
  }

  def appendValue(
    s:     Segment[F],
    key:   Key,
    value: Value
  ): DbScript[F, Offset] = {
    for {
      env <- ask[F, Env[F]]
      recordSize = value.length
      row: Row = Map(
        "recordSize" -> recordSize,
        "value"  -> value
      )
      _         <- env.files.appendToFile(s.path, schema, row)
      sIx       <- env.segment.getOrAddSegmentIx(s)
      offset    <- env.cache.getSegmentOffset(s)
      _         <- env.segmentIx.update(sIx, key, offset)
      newOffset <- env.cache.increaseSegmentOffset(s, recordSize)
    } yield newOffset
  }

  def getOrAddSegmentIx(s: Segment[F]): DbScript[F, SegmentIx[F]] = {
    for {
      env   <- ask[F, Env[F]]
      ixOpt <- env.cache.getSegmentIx(s)
      ix <- ixOpt match {
        case Some(ix) => DbScript.lift(ix.pure[F])
        case None =>
          for {
            ix <- DbScript.lift(SegmentIx.create(s))
            _  <- env.cache.addSegmentIx(s, ix)
          } yield ix
      }
    } yield ix
  }

  def getValue(s: Segment[F], k: Key): DbScript[F, Value] = {
    for {
      env    <- ask[F, Env[F]]
      sIx    <- getOrAddSegmentIx(s)
      offset <- env.cache.getOffsetBySegmentIx(sIx, k)
      record <- BinRecord.read(s.path, offset)
      v      <- DbScript.lift(SegmentRecord.getValue(record))
    } yield v
  }

  def read(filePath: String): DbScript[F, Segment[F]] = {
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
}
