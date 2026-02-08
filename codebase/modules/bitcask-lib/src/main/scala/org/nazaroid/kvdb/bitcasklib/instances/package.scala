package org.nazaroid.kvdb.bitcasklib

import cats.effect.Async
import fs2.io.file.Files
import org.nazaroid.kvdb.bitcasklib.algebra.*

package object instances {

  final class Dsl[F[_]: Async: Files]
      extends FileService[F]
      with CacheService[F]
      with BaseService[F]
      with TblService[F] {}

  final class LibScenariosImpl[F[_]: Async: Files](c: BitcaskConf, s: State[F]) extends LibScenarios[F]:
    override def env: Env[F] = EnvImpl(c, s)

  final class EnvImpl[F[_]: Async: Files](c: BitcaskConf, s: State[F]) extends Env[F]:
    private val dsl = new Dsl()

    override def conf: BitcaskConf = c

    override def files: FileService[F] = dsl

    override def base: BaseService[F] = dsl

    override def tbl: TblService[F] = dsl

    override def cache: CacheService[F] = dsl

    override def state: State[F] = s
}
