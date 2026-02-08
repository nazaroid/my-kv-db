package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async

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
}
