package org.nazaroid.kvdb.bitcask

import cats.data.OptionT
import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.Files
import io.circe.syntax.*
import org.nazaroid.kvdb.bitcask.lib.*
import org.nazaroid.kvdb.core.*
import org.typelevel.log4cats.Logger

/** Bitcask-specific adapter for the core Catalog abstraction. */
final class BitcaskCatalogAdapter[F[_]: Async: Files: Logger](
  bitcaskCatalog: BitcaskCatalog[F])
    extends Catalog[F] {

  override def createDatabase(name: String): F[Database[F]] = {
    for {
      _  <- Logger[F].info(s"Creating database: $name")
      db <- bitcaskCatalog.createDatabase(name)
    } yield new DatabaseWrapper[F](db)
  }

  override def getDatabase(name: String): F[Option[Database[F]]] = {
    bitcaskCatalog.getDatabase(name).map(_.map(new DatabaseWrapper[F](_)))
  }

  override def listDatabases: F[List[String]] = {
    for {
      _       <- Logger[F].debug(s"Listing databases in: ${bitcaskCatalog.rootPath}")
      dbNames <- bitcaskCatalog.listDatabases
    } yield dbNames
  }

  override def deleteDatabase(name: String): F[Unit] = {
    for {
      _ <- Logger[F].info(s"Deleting database: $name")
      _ <- bitcaskCatalog.deleteDatabase(name)

    } yield ()
  }

  override def getStats: F[CatalogStats] = {
    for {
      _            <- Logger[F].debug("Collecting database statistics")
      catalogStats <- bitcaskCatalog.getStats
    } yield CatalogStats(
      totalDatabases = catalogStats.totalDatabases,
      totalTables    = catalogStats.totalTables,
      totalEntries   = catalogStats.totalEntries,
      activeEntries  = catalogStats.activeEntries,
      deletedEntries = catalogStats.deletedEntries,
      totalDataSize  = catalogStats.totalDataSize,
      details = Map(
        "engine_type"     -> "bitcask".asJson,
        "root_path"       -> bitcaskCatalog.rootPath.toString.asJson,
        "database_count"  -> catalogStats.totalDatabases.asJson,
        "total_segments"  -> catalogStats.totalSegments.asJson,
        "active_segments" -> catalogStats.activeSegments.asJson,
        "databases"       -> catalogStats.databaseStats.asJson
      )
    )
  }

  /** Get statistics for a specific database */
  override def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] = {
    (for {
      db      <- OptionT(bitcaskCatalog.getDatabase(dbName))
      dbStats <- OptionT.liftF(db.getStats)
    } yield DatabaseInfo(
      name           = dbStats.name,
      totalTables    = dbStats.totalTables,
      totalEntries   = dbStats.totalEntries,
      activeEntries  = dbStats.activeEntries,
      deletedEntries = dbStats.deletedEntries,
      totalDataSize  = dbStats.totalDataSize,
      details = Map(
        "engine_type"     -> "bitcask".asJson,
        "total_segments"  -> dbStats.totalSegments.asJson,
        "active_segments" -> dbStats.activeSegments.asJson,
        "tables" -> dbStats
          .tableStats
          .map { table =>
            Map(
              "name"            -> table.name.asJson,
              "total_entries"   -> table.totalEntries.asJson,
              "active_entries"  -> table.activeEntries.asJson,
              "deleted_entries" -> table.deletedEntries.asJson,
              "total_data_size" -> table.totalDataSize.asJson,
              "segment_count"   -> table.segmentCount.asJson,
              "active_segments" -> table.activeSegmentCount.asJson,
              "segments" -> table
                .segments
                .map { segment =>
                  Map(
                    "name"             -> segment.name.asJson,
                    "file_size"        -> segment.fileSize.asJson,
                    "is_active"        -> segment.isActive.asJson,
                    "stale_data_ratio" -> segment.staleDataRatio.asJson,
                    "entry_count"      -> segment.entryCount.asJson
                  ).asJson
                }
                .asJson
            ).asJson
          }
          .asJson
      )
    )).value
  }

  /** Get statistics for a specific table in a database */
  override def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]] =
    (for {
      db         <- OptionT(bitcaskCatalog.getDatabase(dbName))
      table      <- OptionT(db.getTable(tableName))
      tableStats <- OptionT.liftF(table.getStats)

    } yield TableInfo(
      name              = tableStats.name,
      entryCount        = tableStats.totalEntries,
      activeEntryCount  = tableStats.activeEntries,
      deletedEntryCount = tableStats.deletedEntries,
      totalDataSize     = tableStats.totalDataSize,
      details = Map(
        "engine_type"          -> "bitcask".asJson,
        "segment_count"        -> tableStats.segmentCount.asJson,
        "active_segment_count" -> tableStats.activeSegmentCount.asJson,
        "segments" -> tableStats
          .segments
          .map { segment =>
            Map(
              "name"             -> segment.name.asJson,
              "file_size"        -> segment.fileSize.asJson,
              "is_active"        -> segment.isActive.asJson,
              "stale_data_ratio" -> segment.staleDataRatio.asJson,
              "entry_count"      -> segment.entryCount.asJson
            ).asJson
          }
          .asJson
      )
    )).value
}

/** Wrapper to adapt BitcaskDatabase to Database interface
  */
class DatabaseWrapper[F[_]: Async: Files: Logger](
  bitcaskDb: BitcaskDatabase[F])
    extends Database[F] {

  override def name: String = bitcaskDb.name

  override def createTable(name: String): F[Table[F]] = {
    bitcaskDb.createTable(name).map(new TableWrapper(_))
  }

  override def getTable(name: String): F[Option[Table[F]]] = {
    bitcaskDb.getTable(name).map(_.map(new TableWrapper(_)))
  }

  override def listTables: F[List[String]] = {
    bitcaskDb.listTables()
  }

  override def deleteTable(name: String): F[Unit] = {
    bitcaskDb.deleteTable(name)
  }
}

/** Wrapper to adapt BitcaskTable to Table interface
  */
class TableWrapper[F[_]: Async: Files: Logger](
  bitcaskTable: BitcaskTable[F])
    extends Table[F] {

  override def name: String = bitcaskTable.name

  override def get(key: String): F[Option[String]] = {
    bitcaskTable.read(key)
  }

  override def set(key: String, value: String): F[Unit] = {
    bitcaskTable.write(key, value) >> ().pure[F]
  }

  override def delete(key: String): F[Unit] = {
    bitcaskTable.delete(key)
  }
}

object BitcaskCatalogAdapter {

  def create[F[_]: Async: Files: Logger](
    config: BitcaskCatalogConfig
  ): Resource[F, BitcaskCatalogAdapter[F]] =
    for {
      c <- BitcaskCatalog.init(config)
    } yield new BitcaskCatalogAdapter[F](c)
}
