package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async
import cats.implicits.given
import fs2.io.file.Files

trait TblService[F[_]: Async: Files] {

  def createIfNotExists(base: Base[F], tblName: TblName): DbScript[F, Tbl[F]] = {
    for {
      env <- ask[F, Env[F]]
      tbl <- DbScript.lift(Tbl.create(base, tblName))
      _   <- env.files.createDirIfNotExists(tbl.path)
      _   <- env.cache.addTbl(base, tbl)
    } yield tbl
  }

  def get(base: Base[F], tblName: TblName): DbScript[F, Tbl[F]] = {
    for {
      env <- ask[F, Env[F]]
      tbl <- env.cache.getTbl(base, tblName)
    } yield tbl
  }

  def writeValue(
    tbl:   Tbl[F],
    key:   Key,
    value: Value
  ): DbScript[F, Unit] = DbScript.lift(tbl.storage.write(key, value))

  def readValue(
    tbl: Tbl[F],
    key:   Key
  ): DbScript[F, Option[Value]] = DbScript.lift(tbl.storage.read(key))

}
