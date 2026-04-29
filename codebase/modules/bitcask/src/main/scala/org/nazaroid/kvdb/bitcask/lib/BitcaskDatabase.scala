package org.nazaroid.kvdb.bitcask.lib

import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.WriteTask
import org.typelevel.log4cats.Logger

final class BitcaskDatabase[F[_]: Async: Files: Logger](
  val name:           String,
  val path:           Path,
  val writeQueue:     Channel[F, WriteTask[F]],
  val configTemplate: BitcaskTableConfig,
  val tables:         Ref[F, Map[String, BitcaskTable[F]]],
  val metricsAdapter: Option[org.nazaroid.kvdb.bitcask.BitcaskPrometheusMetricsAdapter[F]] = None) {

  def getTable(tableName: String): F[Option[BitcaskTable[F]]] = {
    tables.get.map { _.get(tableName) }
  }

  def createTable(tableName: String): F[BitcaskTable[F]] = {
    getTable(tableName).flatMap {
      case Some(sm) => sm.pure[F]
      case None =>
        val tablePath = path / tableName
        for {
          _ <- Files[F].createDirectories(tablePath).handleError(_ => ())
          // Configure storage settings specifically for this table's directory
          tableConfig = configTemplate.copy(folder = tablePath.toString)
          // Initialize storage manager (including recovery from existing files)
          sm <- BitcaskTable.initialize[F](tableName, tableConfig, writeQueue, metricsAdapter)
          _  <- tables.update(_ + (tableName -> sm))
        } yield sm
    }
  }

  /** List all tables (physical directories within the database) */
  def listTables(): F[List[String]] =
    Files[F]
      .list(path)
      .filter(_.extName == "")
      .map(_.fileName.toString)
      .compile
      .toList

  /** Delete a table */
  def deleteTable(tableName: String): F[Unit] =
    tables.update(_ - tableName) *> Files[F].deleteRecursively(path / tableName)

  /** Get database statistics by aggregating all table statistics */
  def getStats: F[BitcaskDatabaseStats] = {
    for {
      tableNames <- listTables()
      allTableStats <- tableNames.flatTraverse { tableName =>
        getTable(tableName).flatMap {
          case Some(table) => table.getStats.map(List(_))
          case None        => Async[F].pure(Nil)
        }
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
      name           = name,
      totalTables    = totalTables,
      totalEntries   = totalEntries,
      activeEntries  = activeEntries,
      deletedEntries = deletedEntries,
      totalDataSize  = totalDataSize,
      totalSegments  = totalSegments,
      activeSegments = activeSegments,
      tableStats     = allTableStats
    )
  }

  /** Load existing tables and their data from filesystem */
  def loadExistingTables: F[Unit] = {
    for {
      _ <- Logger[F].info(s"Loading tables for database: $name")
      tableNames <- listTables()
      _ <- Logger[F].info(s"Found tables: $tableNames")
      
      _ <- tableNames.traverse { tableName =>
        loadTable(tableName)
      }
      
      _ <- Logger[F].info(s"Successfully loaded ${tableNames.size} tables for database: $name")
    } yield ()
  }

  /** Load a single table and its segments */
  private def loadTable(tableName: String): F[BitcaskTable[F]] = {
    for {
      _ <- Logger[F].info(s"Loading table: $tableName")
      tablePath = path / tableName
      
      // Check if table directory exists
      exists <- Files[F].exists(tablePath)
      _ <- if (!exists) {
        Logger[F].warn(s"Table directory $tablePath does not exist, skipping")
      } else Async[F].unit
      
      // Configure table settings
      tableConfig = configTemplate.copy(folder = tablePath.toString)
      
      // Initialize table with existing data (segments will be loaded by BitcaskTable.initialize)
      table <- BitcaskTable.initialize[F](tableName, tableConfig, writeQueue, metricsAdapter)
      
      // Register table
      _ <- tables.update(_ + (tableName -> table))
      
    } yield table
  }
}
