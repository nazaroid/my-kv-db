package org.nazaroid.kvdb.bitcask

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
import org.nazaroid.kvdb.bitcask.lib.{BitcaskCatalogConfig, BitcaskDatabaseStats, BitcaskTableConfig}
import org.nazaroid.kvdb.core.MonitoringConfig
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.file.Files as JFiles
import scala.concurrent.duration.*

sealed class BitcaskStatisticsServiceSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  def withTempDirectory[T](test: Path => IO[T]): IO[T] = {
    IO.delay {
      val tempDir = JFiles.createTempDirectory("kvdb-test")
      Path.fromNioPath(tempDir.toAbsolutePath)
    }.bracket(dir => test(dir))(dir =>
      IO.delay {
        if (JFiles.exists(dir.toNioPath)) {
          JFiles
            .walk(dir.toNioPath)
            .sorted(java.util.Comparator.reverseOrder())
            .forEach(JFiles.delete(_))
        }
      }.handleErrorWith(_ => IO.unit)
    )
  }

  "StatisticsService should collect database information" in {
    withTempDirectory { tempDir =>
      val dataSchema = List(
        FieldDef("valueSize", FieldType.Int32),
        FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
        FieldDef("timestamp", FieldType.Timestamp),
        FieldDef("status", FieldType.RecordStatus),
        FieldDef("crc", FieldType.CRC32)
      )

      val segmentSchema = List(
        FieldDef("keySize", FieldType.Int32),
        FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
        FieldDef("offset", FieldType.Int64),
        FieldDef("timestamp", FieldType.Timestamp),
        FieldDef("status", FieldType.RecordStatus)
      )

      val tableSchema = List(
        FieldDef("keySize", FieldType.Int32),
        FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
        FieldDef("segmentNameSize", FieldType.Int32),
        FieldDef("segmentName", FieldType.StringUtf8(sizeFromField = "segmentNameSize")),
        FieldDef("timestamp", FieldType.Timestamp),
        FieldDef("status", FieldType.RecordStatus)
      )

      val bitcaskTableConfig = BitcaskTableConfig(
        folder          = tempDir.toString,
        maxSegmentSize  = 1024,
        maxSegmentCount = 10,
        dataSchema      = dataSchema,
        segmentSchema   = segmentSchema,
        tableSchema     = tableSchema,
        maxRetries      = 3
      )

      val monitoringConfig = MonitoringConfig(
        checkInterval              = 1.second,
        enableBackgroundMonitoring = false, // Disable for test
        maxStaleRatio              = 0.3,
        compactionThreshold        = 0.5
      )

      catalogResource(tempDir, bitcaskTableConfig).use { catalog =>
        for {
          given Logger[IO]  <- Slf4jLogger.create[IO]
          statisticsService <- BitcaskStatisticsService.create(catalog, monitoringConfig)
          // Add test data
          db    <- catalog.createDatabase("testDb")
          table <- db.createTable("testTable")
          _     <- table.set("key1", "value1")
          _     <- table.set("key2", "value2")
          _     <- table.set("key3", "value3")
          _     <- table.delete("key3")

          result <- statisticsService.getStats

        } yield {
          result.totalDatabases should be(1)
          val databases = result.details("databases").as[List[BitcaskDatabaseStats]].getOrElse(Nil)
          val dbInfo = databases.head
          dbInfo.name should include("test")
          dbInfo.totalEntries should be(3)
          dbInfo.activeEntries should be(2)
          dbInfo.deletedEntries should be(1)
          dbInfo.totalDataSize should be > 0L

          val tableInfo = dbInfo.tableStats.head

          tableInfo.name should be("testTable")
          tableInfo.totalEntries should be(3)
          tableInfo.activeEntries should be(2)
          tableInfo.totalDataSize should be > 0L
          tableInfo.deletedEntries should be(1)
        }
      }
    }
  }

  private def catalogResource(
    tempDir:            Path,
    bitcaskTableConfig: BitcaskTableConfig
  ): Resource[IO, BitcaskCatalogAdapter[IO]] = {
    for {
      given Logger[IO] <- Slf4jLogger.create[IO].toResource
      catalog          <- BitcaskCatalogAdapter.create[IO](BitcaskCatalogConfig(tempDir.toString, bitcaskTableConfig))
    } yield catalog
  }
}
