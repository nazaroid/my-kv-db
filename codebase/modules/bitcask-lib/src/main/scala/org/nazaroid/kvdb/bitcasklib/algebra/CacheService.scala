package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async
import cats.implicits.given

trait CacheService[F[_]: Async] {

  def addBase(base: Base[F]): DbScript[F, BaseRegistry[F]] = {
    for {
      env <- ask[F, Env[F]]
      reg <- DbScript.lift(env.state.registryRef.updateAndGet(_.updated(base.name, base)))
    } yield reg
  }

  def addTbl(base: Base[F], tbl: Tbl[F]): DbScript[F, Base[F]] = {
    for {
      _ <- DbScript.lift(base.tables.update(_.updated(tbl.name, tbl)))
    } yield base
  }

  def getTbl(base: Base[F], tblName: TblName): DbScript[F, Tbl[F]] = {
    for {
      tbl <- DbScript.lift(base.tables.get).map(_(tblName))
    } yield tbl
  }

  def getBase(baseName: BaseName): DbScript[F, Base[F]] = {
    for {
      env  <- ask[F, Env[F]]
      base <- DbScript.lift(env.state.registryRef.get).map(_(baseName))
    } yield base
  }

  def addTblIx(tbl: Tbl[F], ix: TblIx[F]): DbScript[F, Tbl[F]] = {
    for {
      _ <- DbScript.lift(tbl.ix.set(ix.some))
    } yield tbl
  }

  def getTblIx(tbl: Tbl[F]): DbScript[F, Option[TblIx[F]]] = DbScript.lift(tbl.ix.get)

  def addSegmentIx(
    s:  Segment[F],
    ix: SegmentIx[F]
  ): DbScript[F, Segment[F]] = {
    for {
      _ <- DbScript.lift(s.ix.set(ix.some))
    } yield s
  }

  def getSegmentIx(s: Segment[F]): DbScript[F, Option[SegmentIx[F]]] = DbScript.lift(s.ix.get)

  def addSegment(
    tbl: Tbl[F],
    s:   Segment[F]
  ): DbScript[F, Tbl[F]] = {
    for {
      _ <- DbScript.lift(tbl.lastSegment.set(s.some))
    } yield tbl
  }

  def getSegmentOffset(s: Segment[F]): DbScript[F, Offset] = DbScript.lift(s.offset.get)

  def getLastSegment(tbl: Tbl[F]): DbScript[F, Option[Segment[F]]] = DbScript.lift(tbl.lastSegment.get)

  def findSegment(
    tblIx: TblIx[F],
    k:     Key
  ): DbScript[F, Option[Segment[F]]] = {
    for {
      env  <- ask[F, Env[F]]
      sOpt <- DbScript.lift(tblIx.data.get).map(_.get(k))
    } yield sOpt
  }

  def getOffsetBySegmentIx(sIx: SegmentIx[F], k: Key): DbScript[F, Offset] = {
    for {
      env    <- ask[F, Env[F]]
      offset <- DbScript.lift(sIx.data.get).map(_(k))
    } yield offset
  }

  def increaseSegmentOffset(s: Segment[F], delta: Offset): DbScript[F, Offset] = {
    DbScript.lift(s.offset.updateAndGet(_ + delta))
  }

  def updateSegmentIx(
    ix:     SegmentIx[F],
    key:    Key,
    offset: Offset
  ): DbScript[F, SegmentIx[F]] = {
    for {
      _ <- DbScript.lift(ix.data.update(_.updated(key, offset)))
    } yield ix
  }

  def updateTblIx(
    ix:  TblIx[F],
    key: Key,
    s:   Segment[F]
  ): DbScript[F, TblIx[F]] = {
    for {
      _ <- DbScript.lift(ix.data.update(_.updated(key, s)))
    } yield ix
  }
}
