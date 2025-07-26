package org.nazaroid.kvdb.bitcask.lib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async

trait TblIxService[F[_]: Async] {

  def create(tbl: Tbl[F]): DbScript[F, TblIx[F]] = {
    for {
      env <- ask[F, Env[F]]
      ix  <- DbScript.lift(TblIx.create(tbl))
      _   <- env.files.createFile(ix.path)
      _   <- env.cache.addTblIx(tbl, ix)
    } yield ix
  }

  def read(tbl: Tbl[F]): DbScript[F, TblIx[F]] = ???

  def write(ix: TblIx[F]): DbScript[F, TblIx[F]] = ???

  def update(
    ix:  TblIx[F],
    key: Key,
    s:   Segment[F]
  ): DbScript[F, TblIx[F]] = {
    for {
      env    <- ask[F, Env[F]]
      record <- DbScript.lift(TblIxRecord.create(key, s))
      _      <- env.files.appendToFile(ix.path, record)
      _      <- env.cache.updateTblIx(ix, key, s)
    } yield ix
  }

}
