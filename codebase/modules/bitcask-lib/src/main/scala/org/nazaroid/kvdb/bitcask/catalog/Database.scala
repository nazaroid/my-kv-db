package org.nazaroid.kvdb.bitcask.catalog

import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{StorageConfig, StorageManager, WriteTask}

final class Database[F[_]: Async: Files](
  val dbName:         String,
  val dbPath:         Path,
  val writeQueue:     Channel[F, WriteTask[F]],
  val configTemplate: StorageConfig,
  val tables:         Ref[F, Map[String, Table[F]]]) {

  /** Получить таблицу или создать её, если не существует */
  def table(tableName: String): F[Table[F]] = {
    tables.get.flatMap { activeTables =>
      activeTables.get(tableName) match {
        case Some(sm) => Async[F].pure(sm)
        case None =>
          val tablePath = dbPath / tableName
          for {
            _ <- Files[F].createDirectories(tablePath).handleError(_ => ())
            // Настраиваем конфиг конкретно под папку этой таблицы
            tableConfig = configTemplate.copy(folder = tablePath.toString)
            // Инициализируем менеджер (с восстановлением из файлов)
            sm <- StorageManager.initialize[F](tableConfig, writeQueue)
            _  <- tables.update(_ + (tableName -> sm))
          } yield sm
      }
    }
  }

  /** Список всех таблиц (физических папок в БД) */
  def listTables(): Stream[F, String] = Files[F].list(dbPath).filter(_.extName == "").map(_.fileName.toString)

  /** Удаление таблицы */
  def dropTable(tableName: String): F[Unit] =
    tables.update(_ - tableName) *> Files[F].deleteRecursively(dbPath / tableName)
}
