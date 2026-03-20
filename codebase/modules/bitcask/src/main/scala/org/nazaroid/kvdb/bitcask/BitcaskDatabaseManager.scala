package org.nazaroid.kvdb.bitcask

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.{DatabaseManager, Database, DatabaseStats}
import org.nazaroid.kvdb.bitcask.catalog.{Catalog, Database => BitcaskDatabase}
import org.nazaroid.kvdb.bitcask.storage.{StorageManager, Statistics => BitcaskStatistics}
import org.typelevel.log4cats.Logger
import io.circe.syntax._

import scala.collection.mutable

/**
 * Bitcask-specific implementation of DatabaseManager
 * Handles multiple Bitcask databases
 */
class BitcaskDatabaseManager[F[_]: Async: Files: Logger](
  rootPath: String
) extends DatabaseManager[F] {
  
  private val databases = mutable.Map[String, BitcaskDatabase[F]]()
  
  override def createDatabase(name: String): F[Database[F]] = {
    for {
      _ <- Logger[F].info(s"Creating database: $name")
      dbPath = Path(s"$rootPath/$name")
      _ <- Files[F].createDirectories(dbPath)
      
      storageConfig = org.nazaroid.kvdb.bitcask.storage.StorageConfig(
        folder = dbPath.toString,
        maxSegmentSize = 1024 * 1024, // 1MB
        maxSegmentCount = 10,
        dataSchema = createDataSchema(),
        segmentSchema = createSegmentSchema(),
        tableSchema = createTableSchema()
      )
      
      catalog <- Catalog.init(dbPath, storageConfig, 1024, 2)
      db = new BitcaskDatabase[F](name, catalog)
      
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
      _ <- Logger[F].debug(s"Listing databases in: $rootPath")
      rootDirExists <- Files[F].exists(Path(rootPath))
      
      result <- if (rootDirExists) {
        for {
          entries <- Files[F].list(Path(rootPath))
            .filter(Files[F].isDirectory)
            .evalMap(entry => Files[F].fileName(entry))
            .compile
            .toList
        } yield entries
      } else {
        Async[F].pure(List.empty)
      }
    } yield result
  }
  
  override def deleteDatabase(name: String): F[Unit] = {
    for {
      _ <- Logger[F].info(s"Deleting database: $name")
      dbPath = Path(s"$rootPath/$name")
      
      _ <- databases.get(name).traverse_ { db =>
        // Close catalog resources
        db.catalog.stop()
      }
      
      _ <- Async[F].delay {
        databases.remove(name)
      }
      
      _ <- Files[F].deleteIfExists(dbPath)
      
    } yield ()
  }
  
  override def getStats: F[DatabaseStats] = {
    for {
      _ <- Logger[F].debug("Collecting database statistics")
      dbNames <- listDatabases
      
      allDbStats <- dbNames.traverse { dbName =>
        databases.get(dbName).traverse { db =>
          db.catalog.storageManager.getStats.map { stats =>
            dbName -> stats
          }
        }
      }
      
      dbStatsMap = allDbStats.flatten.toMap
      
      totalDatabases = dbStatsMap.size
      totalTables = dbStatsMap.values.map(_.totalTables).sum
      totalEntries = dbStatsMap.values.map(_.totalEntries).sum
      activeEntries = dbStatsMap.values.map(_.activeEntries).sum
      deletedEntries = dbStatsMap.values.map(_.deletedEntries).sum
      totalDataSize = dbStatsMap.values.map(_.totalDataSize).sum
      
      totalSegments = dbStatsMap.values.map(_.segmentStats.size).sum
      activeSegments = dbStatsMap.values.map(_.segmentStats.count(_.isActive)).sum
      
    } yield DatabaseStats(
      totalDatabases = totalDatabases,
      totalTables = totalTables,
      totalEntries = totalEntries,
      activeEntries = activeEntries,
      deletedEntries = deletedEntries,
      totalDataSize = totalDataSize,
      details = Map(
        "engine_type" -> "bitcask".asJson,
        "root_path" -> rootPath.asJson,
        "database_count" -> totalDatabases.asJson,
        "total_segments" -> totalSegments.asJson,
        "active_segments" -> activeSegments.asJson,
        "databases" -> dbStatsMap.map { case (name, stats) =>
          Map(
            "name" -> name.asJson,
            "total_tables" -> stats.totalTables.asJson,
            "total_entries" -> stats.totalEntries.asJson,
            "active_entries" -> stats.activeEntries.asJson,
            "deleted_entries" -> stats.deletedEntries.asJson,
            "total_data_size" -> stats.totalDataSize.asJson,
            "segment_count" -> stats.segmentStats.size.asJson,
            "active_segments" -> stats.segmentStats.count(_.isActive).asJson
          ).asJson
        }.asJson,
        "compression" -> "none".asJson,
        "max_segment_size" -> (1024 * 1024).asJson,
        "max_segment_count" -> 10.asJson
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

/**
 * Wrapper to adapt BitcaskDatabase to Database interface
 */
class DatabaseWrapper[F[_]: Async: Files: Logger](
  bitcaskDb: BitcaskDatabase[F]
) extends Database[F] {
  
  override def name: String = bitcaskDb.name
  
  override def createTable(name: String): F[Unit] = {
    bitcaskDb.table(name)
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

/**
 * Wrapper to adapt BitcaskTable to Table interface  
 */
class TableWrapper[F[_]: Async: Files: Logger](
  bitcaskTable: org.nazaroid.kvdb.bitcask.catalog.Table[F]
) extends Table[F] {
  
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
  def create[F[_]: Async: Files: Logger](rootPath: String): Resource[F, DatabaseManager[F]] = {
    Resource.make(
      Async[F].delay(new BitcaskDatabaseManager[F](rootPath))
    )(_ => Async[F].unit) // TODO: implement proper cleanup
  }
}
