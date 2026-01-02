package org.nazaroid.kvdb.bitcask.lib.algebra

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.Async

trait BaseService[F[_]: Async] {

  def createIfNotExists(rootDir: String, baseName: BaseName): DbScript[F, Base[F]] = {
    for {
      env  <- ask[F, Env[F]]
      base <- DbScript.lift(Base.create(rootDir, baseName))
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
