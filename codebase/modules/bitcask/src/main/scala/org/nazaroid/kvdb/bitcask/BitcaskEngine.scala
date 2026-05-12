package org.nazaroid.kvdb.bitcask

import cats.data.OptionT
import cats.effect
import cats.effect.Async
import cats.effect.implicits.clockOps
import cats.effect.kernel.Resource
import cats.implicits.given
import fs2.io.file.Files
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
import org.nazaroid.kvdb.bitcask.lib.BitcaskTableConfig
import org.nazaroid.kvdb.core.{DatabaseManager, Engine, PerformanceMetricRecorder}
import org.typelevel.log4cats.Logger

object BitcaskEngine {

  def init[F[_]: Async: Files: Logger](
    conf:           BitcaskEngineConfig,
    metricRegistry: CollectorRegistry
  ): Resource[F, Engine[F]] = {
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

    val tableConfig = BitcaskTableConfig(
      folder          = conf.rootDir,
      maxSegmentSize  = conf.maxSegmentSize,
      maxSegmentCount = conf.maxSegmentCount,
      dataSchema      = dataSchema,
      segmentSchema   = segmentSchema,
      tableSchema     = tableSchema
    )

    for {
      databaseManager <- BitcaskDatabaseManager.create[F](conf.rootDir, tableConfig)
      metricRecorder  <- effect.Resource.eval(BitcaskPerformanceMetricRecorder.create[F](metricRegistry))
    } yield BitcaskEngine(databaseManager, metricRecorder)
  }
}

final class BitcaskEngine[F[_]: Async: Logger](
  databaseManager: BitcaskDatabaseManager[F],
  metricRecorder:  PerformanceMetricRecorder[F])
    extends Engine[F] {

  override def dbManager: DatabaseManager[F] = databaseManager

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
  ): F[Option[String]] =
    (for {
      db  <- OptionT(databaseManager.getDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName))
      v   <- OptionT(tbl.get(key))
    } yield v)
      .value
      .timed
      .flatTap { (duration, result) =>
        metricRecorder.recordGetOperation(duration)
      }
      .map { (duration, result) => result }

  override def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] =
    databaseManager
      .getDatabase(baseName)
      .flatMap(_.fold(databaseManager.createDatabase(baseName))(_.pure[F]))
      .flatMap { db =>
        db.getTable(tblName).flatMap(_.fold(db.createTable(tblName))(_.pure[F]))
      }
      .flatMap(_.set(key, value))
      .timed
      .flatTap { (duration, result) =>
        metricRecorder.recordSetOperation(duration)
      }
      .map { (duration, result) => result }

  override def delete(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Unit] =
    (for {
      db  <- OptionT(databaseManager.getDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName))
      _   <- OptionT.liftF(tbl.delete(key))
    } yield ())
      .value
      .void
      .timed
      .flatTap { (duration, result) =>
        metricRecorder.recordDeleteOperation(duration)
      }
      .map { (duration, result) => result }

}
