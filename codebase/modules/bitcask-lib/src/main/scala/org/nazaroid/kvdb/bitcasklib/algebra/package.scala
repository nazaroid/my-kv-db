package org.nazaroid.kvdb.bitcasklib

import cats.data.Kleisli
import cats.effect.{Async, Ref}
import cats.implicits.given

import java.nio.file.{Path, Paths}

package object algebra {
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
  type FileRecord = Array[Byte]

  trait Env[F[_]: Async] {
    def conf: BitcaskConf

    def files: FileService[F]

    def base: BaseService[F]

    def tbl: TblService[F]

    def segment: SegmentService[F]

    def tblIx: TblIxService[F]

    def segmentIx: SegmentIxService[F]

    def cache: CacheService[F]

    def state: State[F]
  }

  final case class DbCatalog()

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
    data: Ref[F, SegmentIxData])

  final case class Tbl[F[_]: Async](
    name:        TblName,
    path:        Path,
    lastSegment: Ref[F, Option[Segment[F]]],
    ix:          Ref[F, Option[TblIx[F]]])

  final case class Segment[F[_]: Async](
    num:        SegmentNum,
    name:       SegmentName,
    path:       Path,
    ix:         Ref[F, Option[SegmentIx[F]]],
    offset:     Ref[F, Offset],
    isReadOnly: Boolean = false)

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

  object TblIx {

    def create[F[_]: Async](tbl: Tbl[F]): F[TblIx[F]] = {
      val name = "table.ix"
      val path = Paths.get(f"${tbl.path.toAbsolutePath}/$name")
      for {
        data <- Async[F].ref(Map.empty[Key, Segment[F]])
      } yield TblIx(name, path, data)
    }
  }

  object Segment {

    def create[F[_]: Async](
      tbl: Tbl[F],
      num: SegmentNum = 1
    ): F[Segment[F]] = {
      val name = f"segment_$num.seg"
      val path = Paths.get(f"${tbl.path.toAbsolutePath}/$name")
      for {
        ix     <- Async[F].ref(Option.empty[SegmentIx[F]])
        offset <- Async[F].ref(0L)
      } yield Segment(num, name, path, ix, offset)
    }
  }

  object SegmentIx {

    def create[F[_]: Async](s: Segment[F]): F[SegmentIx[F]] = {
      val name = f"segment_${s.num}.ix"
      s.path.getFileName
      val path = Paths.get(f"${s.path.getParent.toAbsolutePath}/$name")
      for {
        data <- Async[F].ref(Map.empty[Key, Offset])
      } yield SegmentIx(name, path, data)
    }
  }

  object DbScript {

    extension [F[_]: Async, O](s: DbScript[F, O])

      def run(using env: Env[F]): F[O] = {
        s.run(env)
      }

    extension [F[_]: Async, O](f: F[O])

      def lift: DbScript[F, O] = {
        Kleisli.liftK(f)
      }

  }
}
