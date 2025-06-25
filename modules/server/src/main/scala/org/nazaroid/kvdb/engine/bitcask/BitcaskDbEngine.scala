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
    type DbName = String
    type TblName = String
    type SegmentIxData = Map[Key, Offset]
    type TblIxData = Map[Key, Segment]
    type BaseRegistry = Map[DbName, Base]
    type DbTables = Map[TblName, Tbl]

    final case class Base(
      name:   String,
      path:   Path,
      tables: DbTables)

    final case class TblIx(
      name: String,
      path: Path,
      data: TblIxData)

    final case class SegmentIx(
      name: String,
      path: Path,
      data: SegmentIxData)

    final case class Tbl(
      name:        String,
      path:        Path,
      lastSegment: Segment,
      ix:          TblIx)

    final case class Segment(
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
