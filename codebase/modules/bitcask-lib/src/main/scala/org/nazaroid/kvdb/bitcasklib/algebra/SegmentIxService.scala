package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async
import org.nazaroid.kvdb.bitcasklib.bindata.*

trait SegmentIxService[F[_]: Async] {

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

  def read(tbl: Tbl[F], num: SegmentNum): DbScript[F, SegmentIx[F]] = ???

  def write(ix: SegmentIx[F]): DbScript[F, SegmentIx[F]] = ???
}
