package org.nazaroid.kvdb.bitcask

import cats.data.OptionT
import cats.effect.Async
import cats.effect.implicits.given
import cats.effect.kernel.Resource
import cats.implicits.given
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
import org.nazaroid.kvdb.bitcask.BitcaskEngineConfig
import org.nazaroid.kvdb.core.Engine
import org.typelevel.log4cats.Logger

object BitcaskEngine {

  def init[F[_]: Async: Files: Logger](conf: BitcaskEngineConfig): F[Engine[F]] = {
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

    val storageConfig = BitcaskTableConfig(
      folder          = conf.rootDir,
      maxSegmentSize  = conf.maxSegmentSize,
      maxSegmentCount = conf.maxSegmentCount,
      dataSchema      = dataSchema,
      segmentSchema   = segmentSchema,
      tableSchema     = tableSchema
    )

    for {
      databaseManager <- BitcaskDatabaseManager.create[F](conf.rootDir, storageConfig)
    } yield BitcaskEngine(databaseManager)
  }
}

final class BitcaskEngine[F[_]: Async: Logger](
  databaseManager: BitcaskDatabaseManager[F])
    extends Engine[F] {

  override def createDbIfNotExists(name: String): F[Unit] = {
    databaseManager.createDatabase(name).void
  }

  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    for {
      db <- OptionT(databaseManager.getDatabase(baseName))
        .getOrElseF(databaseManager.createDatabase(baseName))
      _ <- OptionT(db.getTable(tblName)).getOrElseF(db.createTable(tblName))
    } yield ()
  }

  override def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = {
    for {
      db  <- OptionT(databaseManager.getDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName))
    } yield tbl.get(key)
  }

  override def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    for {
      db <- OptionT(databaseManager.getDatabase(baseName))
        .getOrElseF(databaseManager.createDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName)).getOrElseF(db.createTable(tblName))
      _   <- tbl.set(key, value)
    } yield ()
  }

  override def delete(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Unit] = {
    for {
      db  <- OptionT(databaseManager.getDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName))
    } yield tbl.delete(key)
  }
  
}
