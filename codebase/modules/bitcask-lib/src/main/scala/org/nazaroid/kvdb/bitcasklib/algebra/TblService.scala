package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async
import cats.implicits.given

trait TblService[F[_]: Async] {

  def createIfNotExists(base: Base[F], tblName: TblName): DbScript[F, Tbl[F]] = {
    for {
      env <- ask[F, Env[F]]
      tbl <- DbScript.lift(Tbl.create(base, tblName))
      _   <- env.files.createDirIfNotExists(tbl.path)
      _   <- env.cache.addTbl(base, tbl)
      _   <- env.tblIx.create(tbl)
      s   <- env.segment.create(tbl, 1)
      _   <- env.segmentIx.create(s)
    } yield tbl
  }

  def get(base: Base[F], tblName: TblName): DbScript[F, Tbl[F]] = {
    for {
      env <- ask[F, Env[F]]
      tbl <- env.cache.getTbl(base, tblName)
    } yield tbl
  }

  def appendToLastSegment(
    tbl:   Tbl[F],
    key:   Key,
    value: Value
  ): DbScript[F, Segment[F]] = {
    for {
      env <- ask[F, Env[F]]
      s   <- env.tbl.getOrAddLastSegment(tbl)
      _   <- env.segment.appendValue(s, key, value)
      tIx <- env.tbl.getOrAddTblIx(tbl)
      _   <- env.tblIx.update(tIx, key, s)
    } yield s
  }

  private def getOrAddLastSegment(tbl: Tbl[F]): DbScript[F, Segment[F]] = {
    for {
      env  <- ask[F, Env[F]]
      sOpt <- env.cache.getLastSegment(tbl)
      s <- sOpt match {
        case Some(s) => DbScript.lift(s.pure[F])
        case None =>
          for {
            s <- DbScript.lift(Segment.create(tbl))
            _ <- env.cache.addSegment(tbl, s)
          } yield s
      }
    } yield s
  }

  private def getOrAddTblIx(tbl: Tbl[F]): DbScript[F, TblIx[F]] = {
    for {
      env   <- ask[F, Env[F]]
      ixOpt <- env.cache.getTblIx(tbl)
      ix <- ixOpt match {
        case Some(ix) => DbScript.lift(ix.pure[F])
        case None =>
          for {
            ix <- DbScript.lift(TblIx.create(tbl))
            _  <- env.cache.addTblIx(tbl, ix)
          } yield ix
      }
    } yield ix
  }

  def findInSegments(
    tbl: Tbl[F],
    k:   Key
  ): DbScript[F, Option[Value]] = {
    for {
      env <- ask[F, Env[F]]
      tIx <- env.tbl.getOrAddTblIx(tbl)
      vOpt <- env.cache.findSegment(tIx, k) >>= {
        case Some(s) => env.segment.getValue(s, k).map(Some(_))
        case None    => DbScript.lift(None.pure[F])
      }
    } yield vOpt
  }

  def getValue(
    tbl: Tbl[F],
    k:   Key
  ): DbScript[F, Option[Value]] = {
    for {
      env <- ask[F, Env[F]]
      tIx <- env.tbl.getOrAddTblIx(tbl)
      vOpt <- env.cache.findSegment(tIx, k) >>= {
        case Some(s) => env.segment.getValue(s, k).map(Some(_))
        case None    => DbScript.lift(None.pure[F])
      }
    } yield vOpt
  }

}
