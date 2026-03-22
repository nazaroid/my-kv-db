package org.nazaroid.kvdb.bitcask

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.{Files, Path}
import io.circe.syntax.*
import org.nazaroid.kvdb.bitcask.{BitcaskTableConfig, BitcaskDatabase, BitcaskTable, Catalog}
import org.nazaroid.kvdb.core.*
import org.typelevel.log4cats.Logger

import scala.collection.mutable

/** Bitcask-specific implementation of DatabaseManager Handles multiple Bitcask databases
  */
class BitcaskDatabaseManager[F[_]: Async: Files: Logger](
  catalog: Catalog[F])
    extends DatabaseManager[F] {

  private val databases = mutable.Map[String, BitcaskDatabase[F]]()

  override def createDatabase(name: String): F[Database[F]] = {
    for {
      _  <- Logger[F].info(s"Creating database: $name")
      db <- catalog.database(name)
      _ <- Async[F].delay {
        databases(name) = db
      }

    } yield new DatabaseWrapper[F](db)
  }

  override def getDatabase(name: String): F[Option[Database[F]]] = {
    Async[F].delay {
      databases.get(name).map(db => new DatabaseWrapper[F](db))
    }
  }

  override def listDatabases: F[List[String]] = {
    for {
      _       <- Logger[F].debug(s"Listing databases in: ${catalog.rootPath}")
      dbNames <- catalog.listDatabases
    } yield dbNames
  }

  override def deleteDatabase(name: String): F[Unit] = {
    for {
      _ <- Logger[F].info(s"Deleting database: $name")
      _ <- Async[F].delay {
        databases.remove(name)
      }
      dbPath = Path(s"${catalog.rootPath}/$name")
      _ <- Files[F].deleteIfExists(dbPath)

    } yield ()
  }

  override def getStats: F[CatalogStats] = {
    for {
      _ <- Logger[F].debug("Collecting database statistics")

      // Используем правильную иерархию: Catalog -> Database -> Table
      catalogStats <- catalog.getStats

    } yield CatalogStats(
      totalDatabases = catalogStats.totalDatabases,
      totalTables    = catalogStats.totalTables,
      totalEntries   = catalogStats.totalEntries,
      activeEntries  = catalogStats.activeEntries,
      deletedEntries = catalogStats.deletedEntries,
      totalDataSize  = catalogStats.totalDataSize,
      details = Map(
        "engine_type"     -> "bitcask".asJson,
        "root_path"       -> catalog.rootPath.asJson,
        "database_count"  -> catalogStats.totalDatabases.asJson,
        "total_segments"  -> catalogStats.totalSegments.asJson,
        "active_segments" -> catalogStats.activeSegments.asJson,
        "bitcask_stats" -> Map(
          "total_databases" -> catalogStats.totalDatabases.asJson,
          "total_tables"    -> catalogStats.totalTables.asJson,
          "total_segments"  -> catalogStats.totalSegments.asJson,
          "active_segments" -> catalogStats.activeSegments.asJson
        ).asJson,
        "compression"       -> "none".asJson,
        "max_segment_size"  -> (1024 * 1024).asJson,
        "max_segment_count" -> 10.asJson
      )
    )
  }

  /** Get statistics for a specific database */
  override def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] = {
    for {
      db      <- catalog.database(dbName)
      dbStats <- db.getStats

      // Конвертируем BitcaskDatabaseStats в DatabaseInfo
    } yield Some(
      DatabaseInfo(
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
      )
    )
  }

  /** Get statistics for a specific table in a database */
  override def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]] = {
    for {
      db         <- catalog.database(dbName)
      table      <- db.table(tableName)
      tableStats <- table.getStats

      // Конвертируем BitcaskTableStats в TableInfo
    } yield Some(
      TableInfo(
        name           = tableStats.name,
        totalEntries   = tableStats.totalEntries,
        activeEntries  = tableStats.activeEntries,
        deletedEntries = tableStats.deletedEntries,
        totalDataSize  = tableStats.totalDataSize,
        details = Map(
          "engine_type"     -> "bitcask".asJson,
          "segment_count"   -> tableStats.segmentCount.asJson,
          "active_segments" -> tableStats.activeSegmentCount.asJson,
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
      )
    )
  }

  private def createDataSchema() = {
    import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
    List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus),
      FieldDef("crc", FieldType.CRC32)
    )
  }

  private def createSegmentSchema() = {
    import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
    List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("offset", FieldType.Int64),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
    )
  }

  private def createTableSchema() = {
    import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
    List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("segmentNameSize", FieldType.Int32),
      FieldDef("segmentName", FieldType.StringUtf8(sizeFromField = "segmentNameSize")),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
    )
  }
}

/** Wrapper to adapt BitcaskDatabase to Database interface
  */
class DatabaseWrapper[F[_]: Async: Files: Logger](
  bitcaskDb: BitcaskDatabase[F])
    extends Database[F] {

  override def name: String = bitcaskDb.name

  override def createTable(name: String): F[Table[F]] = {
    bitcaskDb.table(name).map(new TableWrapper(_))
  }

  override def getTable(name: String): F[Option[Table[F]]] = {
    bitcaskDb.table(name).map(new TableWrapper(_))
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
  bitcaskTable: org.nazaroid.kvdb.bitcask.BitcaskTable[F])
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

object BitcaskDatabaseManager {

  def create[F[_]: Async: Files: Logger](rootPath: String, configTemplate: BitcaskTableConfig): DatabaseManager[F] = {
    for {
      c <- Catalog.init(rootPath, configTemplate, 1024, 2)
    } yield new BitcaskDatabaseManager[F](c)
  }
}
