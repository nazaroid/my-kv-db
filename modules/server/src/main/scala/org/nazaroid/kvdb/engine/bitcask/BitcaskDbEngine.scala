package org.nazaroid.kvdb.engine.bitcask

import cats.data.Kleisli
import cats.effect.{Async, Ref}
import cats.implicits.*
import org.nazaroid.kvdb.BitcaskConf
import org.nazaroid.kvdb.algebra.{Database, DbEngine, Table}
import org.nazaroid.kvdb.engine.bitcask.BitcaskDbEngine.*
import org.nazaroid.kvdb.engine.bitcask.BitcaskDbEngine.algebra.*

import java.nio.file.{Files, Path, Paths}
// TODO: переделать дизайн, чтобы методы были отделены от данных

final class BitcaskDbEngine[F[_]: Async](conf: BitcaskConf) extends DbEngine[F] {

  override def createDbIfNotExists(name: String): F[Database[F]] = {
    /* TODO:
      создать общий конвейер для всех команд
     * передаем команду в stream с pipe-ом для колбека и ждем ответ
     * при createDbIfNotExists:
       - создать папку для  бд
     */
    val dbPath = Paths.get(f"${conf.rootDir}/$name")
    val db = BitcaskDatabase[F](conf)(dbPath)
    db.init().map(_ => db)
  }
}

object BitcaskDbEngine {

  private class BitcaskDatabase[F[_]: Async](conf: BitcaskConf)(dbPath: Path) extends Database[F] {

    def init(): F[Unit] = {
      Async[F].blocking(Files.createDirectories(dbPath))
    }

    override def createTableIfNotExists(name: String): F[Table[F]] = {
      // TODO:
      // таблицы и базы хранить в памяти, загружать при старте, потом в рилтайме модифицировать список
      // для новой:
      // - создать папку с таблицей внутри папки БД
      // - инициализировать новый пустой сегмент
      //   -- создать файлы индексов (таблицы + сегмента) и сегмента
      // для существующей:
      // - ничего не делать, вернуть объект

      val tblPath = Paths.get(f"${dbPath.toAbsolutePath}/$name")
      val tbl = BitcaskTable[F](conf)(tblPath)
      tbl.init().map(_ => tbl)
    }
  }

  private class BitcaskTable[F[_]: Async](conf: BitcaskConf)(tblPath: Path) extends Table[F] {

    def init(): F[Unit] = {
      Async[F].blocking(Files.createDirectories(tblPath))

    }

    override def get(key: String): F[String] = ???

    override def set(key: String, value: String): F[Unit] = {
      ???
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

    trait Env[F[_]] {
      def conf: BitcaskConf

      def files: FileService[F]

      def base: BaseService[F]

      def tbl: TblService[F]

      def segment: TblSegmentService[F]

      def tblIx: TblIxService[F]

      def segmentIx: SegmentIxService[F]

      def cache: CacheService[F]

      def state: State[F]
    }

    trait CacheService[F[_]] {

      def findSegment(
        baseName: BaseName,
        tblName: TblName,
        k: Key
      ):                                             DbScript[F, Option[Segment]]
      def getOffsetInSegment(sx: SegmentIx, k: Key): DbScript[F, Offset]

      def addBase(base: Base):          DbScript[F, BaseRegistry]
      def addTbl(base: Base, tbl: Tbl): DbScript[F, Base]

      def addSegment(
        env: Env[F]
      )(
        base: Base,
        tbl: Tbl,
        s: Segment
      ): DbScript[F, Base]
    }

    trait FileService[F[_]] {}

    trait BaseService[F[_]] {
      def createIfNotExists(rootDir: String, name: BaseName): DbScript[F, Base]
      def list(rootDir: String):                              DbScript[F, Seq[Base]]
    }

    trait TblService[F[_]] {
      def createIfNotExists(base: Base, name: TblName): DbScript[F, TblIx]
      def list(base: Base):                             DbScript[F, Seq[TblIx]]
    }

    trait TblSegmentService[F[_]] {
      def create(tbl: Tbl, num: SegmentNum):            DbScript[F, Segment]
      def append(s: Segment, batch: Seq[(Key, Value)]): DbScript[F, Segment]
      def readValue(s: Segment, offset: Offset):        DbScript[F, Value]
    }

    trait TblIxService[F[_]] {
      def create(tbl: Tbl): DbScript[F, TblIx]
      def read(tbl: Tbl):   DbScript[F, TblIx]
      def write(ix: TblIx): DbScript[F, TblIx]
    }

    trait SegmentIxService[F[_]] {
      def create(s: Segment):              DbScript[F, SegmentIx]
      def read(tbl: Tbl, num: SegmentNum): DbScript[F, SegmentIx]
      def write(ix: SegmentIx):            DbScript[F, SegmentIx]
    }

    final case class Base(
      name:   BaseName,
      path:   Path,
      tables: BaseTables)

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
      lastSegment: Segment,
      ix:          TblIx)

    final case class Segment(
      num:        SegmentNum,
      name:       String,
      path:       Path,
      isReadOnly: Boolean,
      offset:     Offset,
      ix:         SegmentIx)

    final case class State[F[_]](registryRef: Ref[F, BaseRegistry])

    object DbScript {

      def apply[F[_], O](inner: Env[F] => DbScript[F, O]): DbScript[F, O] =
        Kleisli { (env: Env[F]) =>
          inner(env).run(env)
        }
    }

  }

}

trait BitcaskScenarios[F[_]: Async] {

  def createBaseIfNotExists(baseName: String): DbScript[F, BaseRegistry] = {
    DbScript { (env: Env[F]) =>
      for {
        base     <- env.base.createIfNotExists(env.conf.rootDir, baseName)
        registry <- env.cache.addBase(base)
      } yield registry
    }
  }

}
