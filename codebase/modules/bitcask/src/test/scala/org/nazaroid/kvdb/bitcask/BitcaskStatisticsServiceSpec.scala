package org.nazaroid.kvdb.bitcask

import cats.effect.IO
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType, WriteTask}
import org.nazaroid.kvdb.bitcask.{BitcaskTable, BitcaskTableConfig}
import org.nazaroid.kvdb.core.MonitoringConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.file.Files as JFiles
import scala.concurrent.duration.*

sealed class BitcaskStatisticsServiceSpec extends AnyFunSuite with Matchers {

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

  test("StatisticsService should collect database information") {
    withTempDirectory { tempDir =>
      val dataSchema = List(
        FieldDef("recordSize", FieldType.Int32),
        FieldDef("value", FieldType.StringUtf8(sizeFromField = "recordSize")),
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
      for {
        given Logger[IO]  <- Slf4jLogger.create[IO]
        queue             <- Channel.unbounded[IO, WriteTask[IO]]
        table             <- BitcaskTable.initialize("test_table", bitcaskTableConfig, queue)
        statisticsService <- BitcaskStatisticsService.create(table, monitoringConfig)

        // Add test data
        _ <- table.write("key1", "value1")
        _ <- table.write("key2", "value2")
        _ <- table.write("key3", "value3")
        _ <- table.delete("key3") // Delete one entry

        // Collect statistics
        result <- statisticsService.getDatabases

      } yield {
        result should have size 1
        val dbInfo = result.head
        dbInfo.name should include("test")
        dbInfo.totalEntries should be(3)   // 3 entries total
        dbInfo.activeEntries should be(2)  // 2 active (one deleted)
        dbInfo.deletedEntries should be(1) // 1 deleted
        dbInfo.totalDiskSize should be > 0L
        dbInfo.totalMemorySize should be > 0L
        dbInfo.fragmentationRatio should be >= 0.0
        dbInfo.tables should have size 1

        val tableInfo = dbInfo.tables.head
        tableInfo.name should be("test_table")
        tableInfo.entryCount should be(3)
        tableInfo.activeEntryCount should be(2)
        tableInfo.diskSize should be > 0L
        tableInfo.memorySize should be > 0L
      }
    }
  }

}
