package org.nazaroid.kvdb.engine.bitcask

import cats.effect.{Async, Ref}
import cats.implicits.*
import org.nazaroid.kvdb.BitcaskConf
import org.nazaroid.kvdb.algebra.{Database, DbEngine, Table}
import org.nazaroid.kvdb.engine.bitcask.BitcaskDbEngine.*

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

  private class BitcaskSegment[F[_]: Async](conf: BitcaskConf)() {}

  object algebra {
    type Offset = Long
    type Key = String
    type BaseName = String
    type TblName = String
    type TblIxName = String
    type SegmentNum = Int
    type SegmentIxData = Map[Key, Offset]
    type TblIxData = Map[Key, Segment]
    type BaseRegistry = Map[BaseName, Base]
    type BaseTables = Map[TblName, Tbl]

    trait CacheService[F[_]] {
      def get(): F[State[F]]
    }

    trait FileService[F[_]] {}

    trait Env[F[_]] {
      def files: FileService[F]
      def cache: CacheService[F]
    }

    trait BaseService[F[_]] {
      def createIfNotExists(env: Env[F])(rootDir: String, name: BaseName): F[Base]
    }

    trait TblService[F[_]] {
      def createIfNotExists(env: Env[F])(base: Base, name: TblName): F[TblIx]
    }

    trait TblSegmentService[F[_]] {
      def create(env: Env[F])(tbl: Tbl, num: SegmentNum): F[Segment]
      def write(env: Env[F])(s: Segment):                 F[Segment]
    }

    trait TblIxService[F[_]] {
      def create(env: Env[F])(tbl: Tbl): F[TblIx]
      def write(env: Env[F])(ix: TblIx): F[TblIx]
    }

    trait SegmentIxService[F[_]] {
      def create(env: Env[F])(tbl: Tbl, num: SegmentNum): F[SegmentIx]
      def write(env: Env[F])(ix: SegmentIx):              F[SegmentIx]
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
      ix:         SegmentIx)

    final case class State[F[_]](registryRef: Ref[F, BaseRegistry])
  }

}

trait BitcaskHelper[F[_]: Async] {
  def createDbDir(dbRoot: String, dbName: String): F[Path] = Paths.get(f"$dbRoot/$dbName").pure[F]

}
