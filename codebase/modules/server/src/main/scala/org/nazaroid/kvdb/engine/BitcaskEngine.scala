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

  override def getStats: F[org.nazaroid.kvdb.algebra.DatabaseStats] = {
    c.getStats.map { concreteStats =>
      org.nazaroid.kvdb.algebra.DatabaseStats(
        totalTables = concreteStats.totalTables,
        totalEntries = concreteStats.totalEntries,
        activeEntries = concreteStats.activeEntries,
        deletedEntries = concreteStats.deletedEntries,
        totalDataSize = concreteStats.totalDataSize,
        details = Map(
          "engine_type" -> "bitcask".asJson,
          "table_stats" -> concreteStats.tableStats.map { table =>
            Map(
              "name" -> table.name.asJson,
              "entry_count" -> table.entryCount.asJson,
              "active_entry_count" -> table.activeEntryCount.asJson
            ).asJson
          }.asJson,
          "segment_stats" -> concreteStats.segmentStats.map { segment =>
            Map(
              "name" -> segment.name.asJson,
              "file_size" -> segment.fileSize.asJson,
              "is_active" -> segment.isActive.asJson,
              "stale_data_ratio" -> segment.staleDataRatio.asJson,
              "entry_count" -> segment.entryCount.asJson
            ).asJson
          }.asJson,
          "segment_count" -> concreteStats.segmentStats.size.asJson,
          "active_segment_count" -> concreteStats.segmentStats.count(_.isActive).asJson
        )
      )
    }
  }
}
