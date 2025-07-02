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
        with SegmentService[F]
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

      override def segment: SegmentService[F] = dsl

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

      def segment: SegmentService[F]

      def tblIx: TblIxService[F]

      def segmentIx: SegmentIxService[F]

      def cache: CacheService[F]

      def state: F[State[F]]
    }

    trait CacheService[F[_]: Async] {

      def addBase(base: Base[F]): DbScript[F, BaseRegistry[F]] = {
        for {
          env <- ask[F, Env[F]]
          s   <- DbScript.lift(env.state)
          reg <- DbScript.lift(s.registryRef.updateAndGet(_.updated(base.name, base)))
        } yield reg
      }

      def addTbl(base: Base[F], tbl: Tbl[F]): DbScript[F, Base[F]] = {
        for {
          _ <- DbScript.lift(base.tables.update(_.updated(tbl.name, tbl)))
        } yield base
      }

      def getTbl(baseName: BaseName, tblName: TblName): DbScript[F, Tbl[F]] = {
        for {
          env  <- ask[F, Env[F]]
          base <- getBase(baseName)
          tbl  <- DbScript.lift(base.tables.get).map(_(tblName))
        } yield tbl
      }

      def getBase(baseName: BaseName): DbScript[F, Base[F]] = {
        for {
          env  <- ask[F, Env[F]]
          s    <- DbScript.lift(env.state)
          base <- DbScript.lift(s.registryRef.get).map(_(baseName))
        } yield base
      }

      def addTblIx(tbl: Tbl[F], ix: TblIx[F]): DbScript[F, Tbl[F]] = {
        for {
          _ <- DbScript.lift(tbl.ix.set(ix.some))
        } yield tbl
      }

      def addSegment(
        tbl: Tbl[F],
        s:   Segment[F]
      ): DbScript[F, Tbl[F]] = {
        for {
          _ <- DbScript.lift(tbl.lastSegment.set(s.some))
        } yield tbl
      }

      def addSegmentIx(
        s:  Segment[F],
        ix: SegmentIx[F]
      ): DbScript[F, Segment[F]] = {
        for {
          _ <- DbScript.lift(s.ix.set(ix.some))
        } yield s
      }

      def findSegment(
        baseName: BaseName,
        tblName:  TblName,
        k:        Key
      ): DbScript[F, Option[Segment[F]]] = ???

      def getOffsetInSegment(sx: SegmentIx[F], k: Key): DbScript[F, Offset] = ???
    }

    trait FileService[F[_]: Async] {

      def createDirIfNotExists(path: Path): DbScript[F, Path] = {
        Kleisli { env => Async[F].blocking(Files.createDirectories(path)) }
      }

      def createBinFile(path: Path): DbScript[F, Path] = {
        Kleisli { env => Async[F].blocking(Files.createFile(path)) }
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

      def get(name: BaseName): DbScript[F, Base[F]] = {
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
          tbl <- DbScript.lift(Tbl.create(base, name))
          _   <- env.files.createDirIfNotExists(tbl.path)
          _   <- env.cache.addTbl(base, tbl)
          _   <- env.tblIx.create(tbl)
          s   <- env.segment.create(tbl, 1)
          _   <- env.segmentIx.create(s)
        } yield tbl
      }

      def get(baseName: BaseName, tblName: TblName): DbScript[F, Tbl[F]] = ???

      def list(base: Base[F]): DbScript[F, Seq[Tbl[F]]] = ???
    }

    trait SegmentService[F[_]: Async] {

      def create(tbl: Tbl[F], num: SegmentNum): DbScript[F, Segment[F]] = {
        for {
          env <- ask[F, Env[F]]
          s   <- DbScript.lift(Segment.create(tbl))
          _   <- env.files.createBinFile(s.path)
          _   <- env.cache.addSegment(tbl, s)
        } yield s
      }

      def append(s: Segment[F], batch: Seq[(Key, Value)]): DbScript[F, Segment[F]] = ???

      def readValue(s: Segment[F], offset: Offset): DbScript[F, Value] = ???
    }

    trait TblIxService[F[_]: Async] {

      def create(tbl: Tbl[F]): DbScript[F, TblIx[F]] = {
        for {
          env <- ask[F, Env[F]]
          ix  <- DbScript.lift(TblIx.create(tbl))
          _   <- env.files.createBinFile(ix.path)
          _   <- env.cache.addTblIx(tbl, ix)
        } yield ix
      }

      def read(tbl: Tbl[F]): DbScript[F, TblIx[F]] = ???

      def write(ix: TblIx[F]): DbScript[F, TblIx[F]] = ???
    }

    trait SegmentIxService[F[_]: Async] {

      def create(s: Segment[F]): DbScript[F, SegmentIx[F]] = {
        for {
          env <- ask[F, Env[F]]
          ix  <- DbScript.lift(SegmentIx.create(s))
          _   <- env.files.createBinFile(ix.path)
          _   <- env.cache.addSegmentIx(s, ix)
        } yield ix
      }

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

      def createTableIfNotExists(baseName: String, tblName: String): F[Tbl[F]] =
        DbScript.run {
          for {
            env  <- ask[F, Env[F]]
            base <- env.base.get(baseName)
            tbl  <- env.tbl.createIfNotExists(base, tblName)
            _    <- env.cache.addTbl(base, tbl)
          } yield tbl
        }

      /*
       read
       * по индексу key -> segment находим сегмент
       * по индексу value -> offset находим место записи (см file structure)
       * парсим запись согласно схеме записи


      исключение "в индексе отсутствует"
      • возвращаем None
       */
      def get(
        baseName: String,
        tblName:  String,
        key:      String
      ): F[Option[String]] = ???

      /*
       write
      запись в segment и обновление индексов
      • подбираем сегмент для ключа
      +t (транзакция)
       * добавляем запись в конец сегмента
       * обновляем индекс key -> segment
      • обновляем индекс value -> offset
      -t


      шаг "подбор сегмента"
      нужен последний сегмент, в который можно писать
       * если еще нет сегмента, то создаем новый
       * если последний сегмент is_read_only, то создаем новый
      • иначе пишем в последний сегмент
       */
      def set(
        baseName: String,
        tblName:  String,
        key:      String,
        value:    String
      ): F[Unit] = {
        ???
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
      isReadOnly: Boolean = false,
      offset:     Offset = 0)

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
          ix <- Async[F].ref(Option.empty[SegmentIx[F]])
        } yield Segment(num, name, path, ix)
      }
    }

    object SegmentIx {

      def create[F[_]: Async](s: Segment[F]): F[SegmentIx[F]] = {
        val name = f"${s.name}.ix"
        val path = Paths.get(f"${s.path.toAbsolutePath}/$name")
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
}
