package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async
import fs2.io.file.Files

trait BaseService[F[_]: Async: Files] {

  def createIfNotExists(rootDir: String, baseName: BaseName): DbScript[F, Base[F]] = {
    for {
      env  <- ask[F, Env[F]]
      base <- Base.create(rootDir, baseName)
      _    <- env.files.createDirIfNotExists(base.path)
      _    <- env.cache.addBase(base)
    } yield base
  }

  def get(baseName: BaseName): DbScript[F, Base[F]] = {
    for {
      env  <- ask[F, Env[F]]
      base <- env.cache.getBase(baseName)
    } yield base
  }

}
