package org.nazaroid.kvdb.engine.bitcask

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.{Async, Ref}
import cats.implicits.given
import org.nazaroid.kvdb.BitcaskConf

import java.nio.file.{Files, Path, Paths}

object BitcaskLib {

  import algebra.*
  import instances.*

  def apply[F[_]: Async](c: BitcaskConf): Scenarios[F] = new ScenariosImpl(c)

  object instances {

    final class Dsl[F[_]: Async]
        extends FileService[F]
        with CacheService[F]
        with BaseService[F]
        with TblService[F]
        with TblSegmentService[F]
        with TblIxService[F]
        with SegmentIxService[F] {}

    final class ScenariosImpl[F[_]: Async](c: BitcaskConf) extends Scenarios[F]:
      override def env: Env[F] = EnvImpl(c)

    final class EnvImpl[F[_]: Async](c: BitcaskConf) extends Env[F]:
      private val dsl = new Dsl()

      override def conf: BitcaskConf = c

      override def files: FileService[F] = dsl

      override def base: BaseService[F] = dsl

      override def tbl: TblService[F] = dsl

      override def segment: TblSegmentService[F] = dsl

      override def tblIx: TblIxService[F] = dsl

      override def segmentIx: SegmentIxService[F] = dsl

      override def cache: CacheService[F] = dsl

      override def state: F[State[F]] =
        Async[F].ref(Map.empty[BaseName, Base]) >>= { r =>
          State(r).pure[F]
        }
  }

  object algebra {
    type Offset = Long
    type Key = String
    type Value = String
    type BaseName = String
    type TblName = String
    type TblIxName = String
    type SegmentNum = Int
    type SegmentIxData = Map[Key, Offset]
    type TblIxData = Map[Key, Segment]
    type BaseRegistry = Map[BaseName, Base]
    type BaseTables = Map[TblName, Tbl]
    type DbScript[F[_], O] = Kleisli[F, Env[F], O]

    trait Env[F[_]: Async] {
      def conf: BitcaskConf

      def files: FileService[F]

      def base: BaseService[F]

      def tbl: TblService[F]

      def segment: TblSegmentService[F]

      def tblIx: TblIxService[F]

      def segmentIx: SegmentIxService[F]

      def cache: CacheService[F]

      def state: F[State[F]]
    }

    trait CacheService[F[_]] {

      def findSegment(
        baseName: BaseName,
        tblName:  TblName,
        k:        Key
      ): DbScript[F, Option[Segment]] = ???

      def getOffsetInSegment(sx: SegmentIx, k: Key): DbScript[F, Offset] = ???

      def addBase(base: Base): DbScript[F, BaseRegistry] = ???

      def addTbl(base: Base, tbl: Tbl): DbScript[F, Base] = ???

      def addSegment(
        env: Env[F]
      )(
        base: Base,
        tbl:  Tbl,
        s:    Segment
      ): DbScript[F, Base] = ???
    }

    trait FileService[F[_]: Async] {

      def createDirIfNotExists(path: Path): DbScript[F, Path] = {
        Kleisli { env => Async[F].blocking(Files.createDirectories(path)) }
      }
    }

    trait BaseService[F[_]: Async] {

      def createIfNotExists(rootDir: String, name: BaseName): DbScript[F, Base] = {
        for {
          env <- ask[F, Env[F]]
          dbPath = Paths.get(f"${env.conf.rootDir}/$name")
          base = Base(name, dbPath)
          _ <- env.files.createDirIfNotExists(base.path)
        } yield base
      }

      def list(rootDir: String): DbScript[F, Seq[Base]] = ???
    }

    trait TblService[F[_]: Async] {

      def createIfNotExists(base: Base, name: TblName): DbScript[F, Tbl] = {
        for {
          env <- ask[F, Env[F]]
          // TODO: create segment and ix's
          tblPath = Paths.get(f"${base.path.toAbsolutePath}/$name")
          tbl = Tbl(name, tblPath)
          _ <- env.files.createDirIfNotExists(tblPath)
        } yield tbl
      }

      def list(base: Base): DbScript[F, Seq[Tbl]] = ???
    }

    trait TblSegmentService[F[_]] {
      def create(tbl: Tbl, num: SegmentNum): DbScript[F, Segment] = ???

      def append(s: Segment, batch: Seq[(Key, Value)]): DbScript[F, Segment] = ???

      def readValue(s: Segment, offset: Offset): DbScript[F, Value] = ???
    }

    trait TblIxService[F[_]] {
      def create(tbl: Tbl): DbScript[F, TblIx] = ???

      def read(tbl: Tbl): DbScript[F, TblIx] = ???

      def write(ix: TblIx): DbScript[F, TblIx] = ???
    }

    trait SegmentIxService[F[_]] {
      def create(s: Segment): DbScript[F, SegmentIx] = ???

      def read(tbl: Tbl, num: SegmentNum): DbScript[F, SegmentIx] = ???

      def write(ix: SegmentIx): DbScript[F, SegmentIx] = ???
    }

    trait Scenarios[F[_]: Async] {
      given env: Env[F]

      def createBaseIfNotExists(baseName: String): F[Base] =
        RunDbScript {
          for {
            env  <- ask[F, Env[F]]
            base <- env.base.createIfNotExists(env.conf.rootDir, baseName)
            _    <- env.cache.addBase(base)
          } yield base
        }

      def createTableIfNotExists(base: Base, tblName: String): F[Tbl] =
        RunDbScript {
          for {
            env <- ask[F, Env[F]]
            tbl <- env.tbl.createIfNotExists(base, tblName)
            _   <- env.cache.addTbl(base, tbl)
          } yield tbl
        }
    }

    final case class Base(
      name:   BaseName,
      path:   Path,
      tables: BaseTables = Map.empty)

    final case class TblIx(
      name: TblIxName,
      path: Path,
      data: TblIxData)

    final case class SegmentIx(
      name: String,
      path: Path,
      data: SegmentIxData)

    final case class Tbl(
      name:        TblName,
      path:        Path,
      lastSegment: Option[Segment] = None,
      ix:          Option[TblIx] = None)

    final case class Segment(
      num:        SegmentNum,
      name:       String,
      path:       Path,
      isReadOnly: Boolean,
      offset:     Offset,
      ix:         Option[SegmentIx] = None)

    final case class State[F[_]](registryRef: Ref[F, BaseRegistry])

    object RunDbScript {

      def apply[F[_]: Async, O](s: DbScript[F, O])(using env: Env[F]): F[O] = {
        s.run(env)
      }
    }

  }
}
