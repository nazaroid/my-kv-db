package org.nazaroid.kvdb.bitcask.lib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async
import cats.implicits.given

trait SegmentService[F[_]: Async] {

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
      env       <- ask[F, Env[F]]
      record    <- DbScript.lift(SegmentRecord.create(value))
      _         <- env.files.appendToFile(s.path, record)
      sIx       <- env.segment.getOrAddSegmentIx(s)
      offset    <- env.cache.getSegmentOffset(s)
      _         <- env.segmentIx.update(sIx, key, offset)
      newOffset <- env.cache.increaseSegmentOffset(s, record.size)
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
      record <- env.files.readFileRecord(s.path, offset)
      v      <- DbScript.lift(SegmentRecord.getValue(record))
    } yield v
  }
}
