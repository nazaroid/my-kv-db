package org.nazaroid.kvdb.bitcask

import cats.effect
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.effect.kernel.implicits.effectResourceOps
import fs2.io.file.Files
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
import org.nazaroid.kvdb.bitcask.lib.{BitcaskCatalogConfig, BitcaskTableConfig}
import org.nazaroid.kvdb.core.{CatalogEngine, Engine, MonitoringConfig}
import org.typelevel.log4cats.Logger

object BitcaskEngine {

  private object Schemas {

    // Data files use CRC, segment and table - no
    val data: List[FieldDef] = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus),
      FieldDef("crc", FieldType.CRC32) // CRC only for data
    )

    val segment: List[FieldDef] = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("offset", FieldType.Int64),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
      // No CRC for segment
    )

    val table: List[FieldDef] = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("segmentNameSize", FieldType.Int32),
      FieldDef("segmentName", FieldType.StringUtf8(sizeFromField = "segmentNameSize")),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
      // No CRC for table
    )
  }

  def init[F[_]: Async: Files: Logger](
    conf:           BitcaskEngineConfig,
    metricRegistry: CollectorRegistry
  ): Resource[F, Engine[F]] = {

    val tableConfig = BitcaskTableConfig(
      folder          = conf.rootDir,
      maxSegmentSize  = conf.maxSegmentSize,
      maxSegmentCount = conf.maxSegmentCount,
      dataSchema      = Schemas.data,
      segmentSchema   = Schemas.segment,
      tableSchema     = Schemas.table
    )

    val catalogConfig = BitcaskCatalogConfig(
      rootPath         = conf.rootDir,
      tableConfig      = tableConfig,
      writeBufferSize  = conf.fileWriteBufferSize,
      writeParallelism = conf.fileWriteParallelism
    )

    for {
      catalog        <- BitcaskCatalogAdapter.create[F](catalogConfig)
      metricRecorder <- effect.Resource.eval(BitcaskPerformanceMetricRecorder.create[F](metricRegistry))
      statisticsService <- BitcaskStatisticsService
        .create[F](catalog, MonitoringConfig(), metricRegistry)
        .toResource
    } yield new CatalogEngine(catalog, statisticsService, metricRecorder)
  }
}
