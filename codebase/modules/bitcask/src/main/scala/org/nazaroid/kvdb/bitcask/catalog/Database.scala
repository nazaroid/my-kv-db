package org.nazaroid.kvdb.bitcask.catalog

import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.WriteTask
import org.nazaroid.kvdb.bitcask.storage.{StorageConfig, StorageManager}

final class Database[F[_]: Async: Files](
                                          val dbName:         String,
                                          val dbPath:         Path,
                                          val writeQueue:     Channel[F, WriteTask[F]],
                                          val configTemplate: StorageConfig,
                                          val tables:         Ref[F, Map[String, Table[F]]]) {

  def table(tableName: String): F[Table[F]] = {
    tables.get.flatMap { activeTables =>
      activeTables.get(tableName) match {
        case Some(sm) => Async[F].pure(sm)
        case None =>
          val tablePath = dbPath / tableName
          for {
            _ <- Files[F].createDirectories(tablePath).handleError(_ => ())
            // Configure storage settings specifically for this table's directory
            tableConfig = configTemplate.copy(folder = tablePath.toString)
            // Initialize storage manager (including recovery from existing files)
            sm <- StorageManager.initialize[F](tableConfig, writeQueue)
            _  <- tables.update(_ + (tableName -> sm))
          } yield sm
      }
    }
  }

  /** List all tables (physical directories within the database) */
  def listTables(): Stream[F, String] = Files[F].list(dbPath).filter(_.extName == "").map(_.fileName.toString)

  /** Delete a table */
  def dropTable(tableName: String): F[Unit] =
    tables.update(_ - tableName) *> Files[F].deleteRecursively(dbPath / tableName)
}
