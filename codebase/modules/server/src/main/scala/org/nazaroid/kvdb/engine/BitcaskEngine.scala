package org.nazaroid.kvdb.engine

import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits.given
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.EngineConfig
import org.nazaroid.kvdb.algebra.Engine
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
import org.nazaroid.kvdb.bitcask.catalog.Catalog
import org.nazaroid.kvdb.bitcask.storage.{StorageConfig, StorageManager}
import org.typelevel.log4cats.Logger

object BitcaskEngine {

  def init[F[_]: Async: Files: Logger](conf: EngineConfig): Resource[F, Engine[F]] = {
    // Data files use CRC, segment and table - no
    val dataSchema = List(
      FieldDef("recordSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "recordSize")),
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
      c <- Catalog.init(Path(conf.rootDir), storageConfig, conf.fileWriteBufferSize, conf.fileWriteParallelism)
    } yield BitcaskEngine(c)
  }
}

final class BitcaskEngine[F[_]: Async: Logger](c: Catalog[F]) extends Engine[F] {

  override def createDbIfNotExists(name: String): F[Unit] = {
    for {
      _ <- c.database(name)
    } yield ()
  }

  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    for {
      db <- c.database(baseName)
      _  <- db.table(tblName)
    } yield ()
  }

  override def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = {
    for {
      db   <- c.database(baseName)
      tbl  <- db.table(tblName)
      vOpt <- tbl.read(key)
    } yield vOpt
  }

  override def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    for {
      db     <- c.database(baseName)
      tbl    <- db.table(tblName)
      result <- tbl.write(key, value)
      _ <- result match {
        case Right(()) => Async[F].unit
        case Left(error) =>
          Logger[F].error(s"Failed to set key $key in table $tblName: $error") *>
            Async[F].raiseError(new RuntimeException(s"Write operation failed: $error"))
      }
    } yield ()
  }

  override def delete(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Unit] = {
    for {
      db  <- c.database(baseName)
      tbl <- db.table(tblName)
      _   <- tbl.delete(key)
    } yield ()
  }
}
