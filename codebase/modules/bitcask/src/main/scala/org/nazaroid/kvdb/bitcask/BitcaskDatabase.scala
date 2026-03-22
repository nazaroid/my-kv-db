package org.nazaroid.kvdb.bitcask

import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.WriteTask
import org.typelevel.log4cats.Logger

final class BitcaskDatabase[F[_]: Async: Files: Logger](
  val dbName:         String,
  val dbPath:         Path,
  val writeQueue:     Channel[F, WriteTask[F]],
  val configTemplate: BitcaskTableConfig,
  val tables:         Ref[F, Map[String, BitcaskTable[F]]]) {

  def table(tableName: String): F[BitcaskTable[F]] = {
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
            sm <- BitcaskTable.initialize[F](tableConfig, writeQueue)
            _  <- tables.update(_ + (tableName -> sm))
          } yield sm
      }
    }
  }

  /** List all tables (physical directories within the database) */
  def listTables(): F[List[String]] =
    Files[F]
      .list(dbPath)
      .filter(_.extName == "")
      .map(_.fileName.toString)
      .compile
      .toList

  /** Delete a table */
  def dropTable(tableName: String): F[Unit] =
    tables.update(_ - tableName) *> Files[F].deleteRecursively(dbPath / tableName)
  
  /** Get database statistics by aggregating all table statistics */
  def getStats: F[BitcaskDatabaseStats] = {
    for {
      tableNames <- listTables()
      allTableStats <- tableNames.traverse { tableName =>
        table(tableName).flatMap(_.getStats)
      }
      
      // Aggregate statistics from all tables
      totalTables = allTableStats.size
      totalEntries = allTableStats.map(_.totalEntries).sum
      activeEntries = allTableStats.map(_.activeEntries).sum
      deletedEntries = allTableStats.map(_.deletedEntries).sum
      totalDataSize = allTableStats.map(_.totalDataSize).sum
      totalSegments = allTableStats.map(_.segmentCount).sum
      activeSegments = allTableStats.map(_.activeSegmentCount).sum
      
    } yield BitcaskDatabaseStats(
      name = dbName,
      totalTables = totalTables,
      totalEntries = totalEntries,
      activeEntries = activeEntries,
      deletedEntries = deletedEntries,
      totalDataSize = totalDataSize,
      totalSegments = totalSegments,
      activeSegments = activeSegments,
      tableStats = allTableStats
    )
  }
}
