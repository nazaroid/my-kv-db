package org.nazaroid.kvdb.bitcask

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.{Files, Path}
import io.circe.syntax.*
import org.nazaroid.kvdb.bitcask.catalog.{BitcaskDatabase, BitcaskTable, Catalog}
import org.nazaroid.kvdb.bitcask.storage.{StorageConfig, StorageManager}
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

  override def getStats: F[DatabaseStats] = {
    for {
      _       <- Logger[F].debug("Collecting database statistics")
      dbNames <- listDatabases

      allDbStats <- dbNames.traverse { dbName =>
        databases.get(dbName).traverse { db =>
          for {
            tableNames <- db.listTables().compile.toList
            allTableStats <- tableNames.traverse { tableName =>
              for {
                tbl        <- db.table(tableName)
                tableStats <- getTableStats(table)
              } yield tableName -> tableStats
            }
            tableStatsMap = allTableStats.flatten.toMap

            totalEntries = tableStatsMap.values.map(_.totalEntries).sum
            activeEntries = tableStatsMap.values.map(_.activeEntries).sum
            deletedEntries = tableStatsMap.values.map(_.deletedEntries).sum
            totalDataSize = tableStatsMap.values.map(_.totalDataSize).sum

          } yield DatabaseInfo(
            name           = dbName,
            totalTables    = tableStatsMap.size,
            totalEntries   = totalEntries,
            activeEntries  = activeEntries,
            deletedEntries = deletedEntries,
            totalDataSize  = totalDataSize,
            details = Map(
              "tables" -> tableStatsMap.map { case (name, stats) =>
                Map(
                  "name"            -> name.asJson,
                  "total_entries"   -> stats.totalEntries.asJson,
                  "active_entries"  -> stats.activeEntries.asJson,
                  "deleted_entries" -> stats.deletedEntries.asJson,
                  "total_data_size" -> stats.totalDataSize.asJson
                ).asJson
              }.asJson
            )
          )
        }
      }
      dbStatsMap = allDbStats.flatten.toMap

      totalDatabases = dbStatsMap.size
      totalTables = dbStatsMap.values.map(_.totalTables).sum
      totalEntries = dbStatsMap.values.map(_.totalEntries).sum
      activeEntries = dbStatsMap.values.map(_.activeEntries).sum
      deletedEntries = dbStatsMap.values.map(_.deletedEntries).sum
      totalDataSize = dbStatsMap.values.map(_.totalDataSize).sum

    } yield DatabaseStats(
      totalDatabases = totalDatabases,
      totalTables    = totalTables,
      totalEntries   = totalEntries,
      activeEntries  = activeEntries,
      deletedEntries = deletedEntries,
      totalDataSize  = totalDataSize,
      details = Map(
        "engine_type"    -> "bitcask".asJson,
        "root_path"      -> rootPath.asJson,
        "database_count" -> totalDatabases.asJson,
        "databases" -> dbStatsMap.map { case (name, stats) =>
          Map(
            "name"            -> name.asJson,
            "total_tables"    -> stats.totalTables.asJson,
            "total_entries"   -> stats.totalEntries.asJson,
            "active_entries"  -> stats.activeEntries.asJson,
            "deleted_entries" -> stats.deletedEntries.asJson,
            "total_data_size" -> stats.totalDataSize.asJson,
            "tables"          -> stats.details("tables")
          ).asJson
        }.asJson,
        "compression"       -> "none".asJson,
        "max_segment_size"  -> (1024 * 1024).asJson,
        "max_segment_count" -> 10.asJson
      )
    )
  }

  /** Get statistics for a single table through its StorageManager */
  private def getTableStats(bitcaskTable: BitcaskTable[F]): F[TableInfo] = {
    bitcaskTable.getStats.map { stats =>
      TableInfo(
        name           = Path(bitcaskTable.tableStorage.filePath).fileName,
        totalEntries   = stats.totalEntries,
        activeEntries  = stats.activeEntries,
        deletedEntries = stats.deletedEntries,
        totalDataSize  = stats.totalDataSize,
        details        = stats.details
      )
    }
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

  override def createTable(name: String): F[BitcaskTable[F]] = {
    bitcaskDb.table(name).map(new TableWrapper(_))
  }

  override def getTable(name: String): F[Option[BitcaskTable[F]]] = {
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
  bitcaskTable: org.nazaroid.kvdb.bitcask.catalog.BitcaskTable[F])
    extends Table[F] {

  override def name: String = bitcaskTable.name

  override def get(key: String): F[Option[String]] = {
    bitcaskTable.read(key)
  }

  override def set(key: String, value: String): F[Unit] = {
    bitcaskTable.write(key, value)
  }

  override def delete(key: String): F[Unit] = {
    bitcaskTable.delete(key)
  }

  override def listKeys: F[List[String]] = {
    bitcaskTable.listKeys()
  }
}

object BitcaskDatabaseManager {

  def create[F[_]: Async: Files: Logger](rootPath: String, configTemplate: StorageConfig): DatabaseManager[F] = {
    for {
      c <- Catalog.init(rootPath, storageConfig, 1024, 2)
    } yield new BitcaskDatabaseManager[F](c)
  }
}
