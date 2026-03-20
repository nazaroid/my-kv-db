package org.nazaroid.kvdb.database

import cats.effect.{Async, Resource}
import cats.implicits.given
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.bitcask.BitcaskDatabaseManager
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
import org.nazaroid.kvdb.bitcask.storage.StorageConfig
import org.typelevel.log4cats.Logger

/**
 * Engine configuration
 */
case class EngineConfig(
  rootDir:               String,
  maxSegmentSize:        Long,
  maxSegmentCount:       Int,
  fileWriteBufferSize:   Int,
  fileWriteParallelism:  Int
)

/**
 * Bitcask-specific implementation of Engine
 * Now depends on database module, not server module
 */
final class BitcaskEngine[F[_]: Async: Logger](
  databaseManager: BitcaskDatabaseManager[F]
) extends Engine[F] {

  override def createDbIfNotExists(name: String): F[Unit] = {
    databaseManager.createDatabase(name).void
  }

  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    for {
      db <- databaseManager.getDatabase(baseName)
      _ <- db match {
        case Some(database) => database.createTable(tblName)
        case None => 
          databaseManager.createDatabase(baseName) *>
          databaseManager.getDatabase(baseName).flatMap(_.createTable(tblName))
      }
    } yield ()
  }

  override def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = {
    databaseManager.getDatabase(baseName).flatMap {
      case Some(database) => 
        database.getTable(tblName).flatMap(_.get(key))
      case None => Async[F].pure(None)
    }
  }

  override def set(
    baseName: String,
    tblName: String,
    key:      String,
    value:    String
  ): F[Unit] = {
    databaseManager.getDatabase(baseName).flatMap {
      case Some(database) => 
        database.getTable(tblName).flatMap(_.set(key, value))
      case None => 
        databaseManager.createDatabase(baseName) *>
          databaseManager.getDatabase(baseName).flatMap(_.getTable(tblName).flatMap(_.set(key, value)))
      }
    } yield ()
  }

  override def delete(
    baseName: String,
    tblName: String,
    key:      String
  ): F[Unit] = {
    databaseManager.getDatabase(baseName).flatMap {
      case Some(database) => 
        database.getTable(tblName).flatMap(_.delete(key))
      case None => Async[F].unit
    }
  }
  
  override def getStats: F[DatabaseStats] = {
    databaseManager.getStats
  }
}

object BitcaskEngine {

  def init[F[_]: Async: Files: Logger](conf: EngineConfig): Resource[F, Engine[F]] = {
    // Data files use CRC, segment and table - no
    val dataSchema = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus),
      FieldDef("crc", FieldType.CRC32) // CRC only for data
    )
    val segmentSchema = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("offset", FieldType.Int64),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
      // No CRC for segment
    )
    val tableSchema = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("segmentNameSize", FieldType.Int32),
      FieldDef("segmentName", FieldType.StringUtf8(sizeFromField = "segmentNameSize")),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
      // No CRC for table
    )

    val storageConfig = StorageConfig(
      folder          = conf.rootDir,
      maxSegmentSize  = conf.maxSegmentSize,
      maxSegmentCount = conf.maxSegmentCount,
      dataSchema      = dataSchema,
      segmentSchema   = segmentSchema,
      tableSchema     = tableSchema
    )
    
    for {
      databaseManager <- BitcaskDatabaseManager.create[F](conf.rootDir)
    } yield BitcaskEngine(databaseManager)
  }
}
