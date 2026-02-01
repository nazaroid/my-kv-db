package org.nazaroid.kvdb.bitcasklib

import cats.effect.Async
import org.nazaroid.kvdb.bitcasklib.algebra.*

package object instances {

  final class Dsl[F[_]: Async]
      extends FileService[F]
      with CacheService[F]
      with BaseService[F]
      with TblService[F]
      with SegmentService[F]
      with TblIxService[F]
      with SegmentIxService[F] {}

  final class LibScenariosImpl[F[_]: Async](c: BitcaskConf, s: State[F]) extends LibScenarios[F]:
    override def env: Env[F] = EnvImpl(c, s)

  final class EnvImpl[F[_]: Async](c: BitcaskConf, s: State[F]) extends Env[F]:
    private val dsl = new Dsl()

    override def conf: BitcaskConf = c

    override def files: FileService[F] = dsl

    override def base: BaseService[F] = dsl

    override def tbl: TblService[F] = dsl

    override def segment: SegmentService[F] = dsl

    override def tblIx: TblIxService[F] = dsl

    override def segmentIx: SegmentIxService[F] = dsl

    override def cache: CacheService[F] = dsl

    override def state: State[F] = s
}
