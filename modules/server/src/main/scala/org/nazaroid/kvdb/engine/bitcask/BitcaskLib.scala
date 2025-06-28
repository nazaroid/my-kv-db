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

  def apply[F[_]: Async](c: BitcaskConf): LibScenarios[F] = new LibScenariosImpl(c)

  object instances {

    final class Dsl[F[_]: Async]
        extends FileService[F]
        with CacheService[F]
        with BaseService[F]
        with TblService[F]
        with TblSegmentService[F]
        with TblIxService[F]
        with SegmentIxService[F] {}

    final class LibScenariosImpl[F[_]: Async](c: BitcaskConf) extends LibScenarios[F]:
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
        Async[F].ref(Map.empty[BaseName, Base[F]]) >>= { r =>
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
    type SegmentName = String
    type SegmentIxName = String
    type SegmentIxData = Map[Key, Offset]
    type TblIxData[F[_]] = Map[Key, Segment[F]]
    type BaseRegistry[F[_]] = Map[BaseName, Base[F]]
    type BaseTables[F[_]] = Map[TblName, Tbl[F]]
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

    trait CacheService[F[_]: Async] {

      def findSegment(
        baseName: BaseName,
        tblName:  TblName,
        k:        Key
      ): DbScript[F, Option[Segment[F]]] = ???

      def getOffsetInSegment(sx: SegmentIx[F], k: Key): DbScript[F, Offset] = ???

      def addBase(base: Base[F]): DbScript[F, BaseRegistry[F]] = {
        for {
          env <- ask[F, Env[F]]
          s   <- Kleisli.liftK(env.state)
          reg <- Kleisli.liftK(s.registryRef.updateAndGet(_.updated(base.name, base)))
        } yield reg
      }

      def addTbl(base: Base[F], tbl: Tbl[F]): DbScript[F, Base[F]] = ???

      def addSegment(
        env: Env[F]
      )(
        base: Base[F],
        tbl:  Tbl[F],
        s:    Segment[F]
      ): DbScript[F, Base[F]] = ???
    }

    trait FileService[F[_]: Async] {

      def createDirIfNotExists(path: Path): DbScript[F, Path] = {
        Kleisli { env => Async[F].blocking(Files.createDirectories(path)) }
      }
    }

    trait BaseService[F[_]: Async] {

      def createIfNotExists(rootDir: String, name: BaseName): DbScript[F, Base[F]] = {
        for {
          env  <- ask[F, Env[F]]
          base <- DbScript.lift(Base.create(env.conf.rootDir, name))
          _    <- env.files.createDirIfNotExists(base.path)
          _    <- env.cache.addBase(base)
        } yield base
      }

      def list(rootDir: String): DbScript[F, Seq[Base[F]]] = ???
    }

    trait TblService[F[_]: Async] {

      def createIfNotExists(base: Base[F], name: TblName): DbScript[F, Tbl[F]] = {
        for {
          env <- ask[F, Env[F]]
          // TODO: create segment and ix's
          tblPath = Paths.get(f"${base.path.toAbsolutePath}/$name")
          _   <- env.files.createDirIfNotExists(tblPath)
          tbl <- DbScript.lift(Tbl.create(base, name))
          _   <- env.cache.addTbl(base, tbl)
          s   <- env.segment.create(tbl, 1)
          _   <- env.segmentIx.create(s)
        } yield tbl
      }

      def list(base: Base[F]): DbScript[F, Seq[Tbl[F]]] = ???
    }

    trait TblSegmentService[F[_]] {
      def create(tbl: Tbl[F], num: SegmentNum): DbScript[F, Segment[F]] = ???

      def append(s: Segment[F], batch: Seq[(Key, Value)]): DbScript[F, Segment[F]] = ???

      def readValue(s: Segment[F], offset: Offset): DbScript[F, Value] = ???
    }

    trait TblIxService[F[_]: Async] {
      def create(tbl: Tbl[F]): DbScript[F, TblIx[F]] = ???

      def read(tbl: Tbl[F]): DbScript[F, TblIx[F]] = ???

      def write(ix: TblIx[F]): DbScript[F, TblIx[F]] = ???
    }

    trait SegmentIxService[F[_]: Async] {
      def create(s: Segment[F]): DbScript[F, SegmentIx[F]] = ???

      def read(tbl: Tbl[F], num: SegmentNum): DbScript[F, SegmentIx[F]] = ???

      def write(ix: SegmentIx[F]): DbScript[F, SegmentIx[F]] = ???
    }

    trait LibScenarios[F[_]: Async] {
      given env: Env[F]

      def createBaseIfNotExists(baseName: String): F[Base[F]] =
        DbScript.run {
          for {
            env  <- ask[F, Env[F]]
            base <- env.base.createIfNotExists(env.conf.rootDir, baseName)
            _    <- env.cache.addBase(base)
          } yield base
        }

      def createTableIfNotExists(base: Base[F], tblName: String): F[Tbl[F]] =
        DbScript.run {
          for {
            env <- ask[F, Env[F]]
            tbl <- env.tbl.createIfNotExists(base, tblName)
            _   <- env.cache.addTbl(base, tbl)
          } yield tbl
        }
    }

    final case class Base[F[_]: Async](
      name:   BaseName,
      path:   Path,
      tables: Ref[F, BaseTables[F]])

    final case class TblIx[F[_]: Async](
      name: TblIxName,
      path: Path,
      data: Ref[F, TblIxData[F]])

    final case class SegmentIx[F[_]: Async](
      name: SegmentIxName,
      path: Path,
      data: F[Ref[F, SegmentIxData]])

    final case class Tbl[F[_]: Async](
      name:        TblName,
      path:        Path,
      lastSegment: Ref[F, Option[Segment[F]]],
      ix:          Ref[F, Option[TblIx[F]]])

    final case class Segment[F[_]: Async](
      num:        SegmentNum,
      name:       SegmentName,
      path:       Path,
      isReadOnly: Boolean,
      offset:     Offset,
      ix:         Ref[F, Option[SegmentIx[F]]])

    final case class State[F[_]: Async](registryRef: Ref[F, BaseRegistry[F]])

    object Tbl {

      def create[F[_]: Async](base: Base[F], name: TblName): F[Tbl[F]] = {
        val path = Paths.get(f"${base.path.toAbsolutePath}/$name")
        for {
          lastSegment <- Async[F].ref(Option.empty[Segment[F]])
          ix          <- Async[F].ref(Option.empty[TblIx[F]])
        } yield Tbl(name, path, lastSegment, ix)
      }
    }

    object Base {

      def create[F[_]: Async](rootDir: String, name: BaseName): F[Base[F]] = {
        val path = Paths.get(f"${rootDir}/$name")
        for {
          tables <- Async[F].ref(Map.empty[TblName, Tbl[F]])
        } yield Base(name, path, tables)
      }
    }

    object DbScript {

      def run[F[_]: Async, O](s: DbScript[F, O])(using env: Env[F]): F[O] = {
        s.run(env)
      }

      def lift[F[_]: Async, O](f: F[O]): DbScript[F, O] = ???
    }

  }
}
