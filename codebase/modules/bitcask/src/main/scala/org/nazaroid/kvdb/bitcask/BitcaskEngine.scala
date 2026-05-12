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

import scala.concurrent.duration.FiniteDuration

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
  ): F[Option[String]] = {
    val operation = (for {
      db  <- OptionT(databaseManager.getDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName))
      v   <- OptionT(tbl.get(key))
    } yield v).value

    for {
      timedResult <- operation.timed
      (duration, result) = timedResult
      _ <- metricRecorder.recordGetOperation(duration)
    } yield result
  }

  override def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    val operation = for {
      db <- databaseManager.getDatabase(baseName).flatMap {
        case Some(d) => d.pure[F]
        case None    => databaseManager.createDatabase(baseName)
      }
      tbl <- db.getTable(tblName).flatMap {
        case Some(t) => t.pure[F]
        case None    => db.createTable(tblName)
      }
      _ <- tbl.set(key, value)
    } yield ()

    operation.timed.flatMap { case (duration, _) =>
      metricRecorder.recordSetOperation(duration)
    }
  }

  override def delete(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Unit] = {
    val operation = (for {
      db  <- OptionT(databaseManager.getDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName))
      _   <- OptionT.liftF(tbl.delete(key))
    } yield ()).value.void

    operation.timed.flatMap { case (duration, _) =>
      metricRecorder.recordDeleteOperation(duration)
    }
  }

}
