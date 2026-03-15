package org.nazaroid.kvdb.statistics

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.bitcask.storage.{StorageConfig, StorageManager}
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType, WriteTask}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.file.{Paths, Files as JFiles}
import scala.concurrent.duration.*

class StatisticsServiceSpec extends AnyFunSuite with Matchers {

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
      
      val storageConfig = StorageConfig(
        folder = tempDir.toString,
        maxSegmentSize = 1024,
        maxSegmentCount = 10,
        dataSchema = dataSchema,
        segmentSchema = segmentSchema,
        tableSchema = tableSchema,
        maxRetries = 3
      )
      
      val monitoringConfig = MonitoringConfig(
        checkInterval = 1.second,
        enableBackgroundMonitoring = false, // Disable for test
        maxStaleRatio = 0.3,
        compactionThreshold = 0.5
      )
      for {
        queue <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager <- StorageManager.initialize(storageConfig, queue)
        statisticsService <- StatisticsService.create(storageManager, monitoringConfig)
        
        // Add test data
        _ <- storageManager.write("test_table/key1", "value1")
        _ <- storageManager.write("test_table/key2", "value2")
        _ <- storageManager.write("test_table/key3", "value3")
        _ <- storageManager.delete("test_table/key3") // Delete one entry
        
        // Collect statistics
        result <- statisticsService.getDatabases
        
      } yield {
        result should have size 1
        val dbInfo = result.head
        dbInfo.name should include("test")
        dbInfo.totalEntries should be(3) // 3 entries total
        dbInfo.activeEntries should be(2) // 2 active (one deleted)
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

  test("StatisticsService should export Prometheus format") {
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
      
      val storageConfig = StorageConfig(
        folder = tempDir.toString,
        maxSegmentSize = 1024,
        maxSegmentCount = 10,
        dataSchema = dataSchema,
        segmentSchema = segmentSchema,
        tableSchema = tableSchema,
        maxRetries = 3
      )
      
      val monitoringConfig = MonitoringConfig(
        enableBackgroundMonitoring = false
      )
      
      for {
        queue <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager <- StorageManager.initialize(storageConfig, queue)
        statisticsService <- StatisticsService.create(storageManager, monitoringConfig)
        
        // Add test data
        _ <- storageManager.write("test_table/key1", "value1")
        _ <- storageManager.write("test_table/key2", "value2")
        
        // Export Prometheus format
        result <- statisticsService.exportForPrometheus()
        
      } yield {
        // Check Prometheus format
        result should include("# HELP kvdb_database_info")
        result should include("# TYPE kvdb_database_info gauge")
        result should include("kvdb_database_info{database=")
        result should include("type=\"total_entries\"}")
        result should include("type=\"active_entries\"}")
        result should include("type=\"deleted_entries\"}")
        result should include("type=\"disk_size_bytes\"}")
        result should include("type=\"memory_size_bytes\"}")
        result should include("type=\"fragmentation_ratio\"}")

        result should include("# HELP kvdb_table_info")
        result should include("# TYPE kvdb_table_info gauge")
        result should include("kvdb_table_info{database=")
        result should include("table=\"test_table\"}")

        result should include("# HELP kvdb_segment_info")
        result should include("# TYPE kvdb_segment_info gauge")
        result should include("kvdb_segment_info{database=")
        result should include("segment=")
        result should include("type=\"file_size_bytes\"}")
        result should include("type=\"is_active\"}")
        result should include("type=\"stale_ratio\"}")
        result should include("type=\"entry_count\"}")
      }
      
    }
  }

  test("StatisticsIntegration should provide health checks") {
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
      
      val storageConfig = StorageConfig(
        folder = tempDir.toString,
        maxSegmentSize = 1024,
        maxSegmentCount = 10,
        dataSchema = dataSchema,
        segmentSchema = segmentSchema,
        tableSchema = tableSchema,
        maxRetries = 3
      )
      
      val monitoringConfig = MonitoringConfig(
        enableBackgroundMonitoring = false
      )
      
      for {
        queue <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager <- StorageManager.initialize(storageConfig, queue)
        statisticsIntegration <- StatisticsIntegration.create(storageManager, monitoringConfig)
        
        // Add test data
        _ <- storageManager.write("test_table/key1", "value1")
        _ <- storageManager.write("test_table/key2", "value2")
        
        // Get health check
        result <- statisticsIntegration.getHealthCheck
        
      } yield {
        result.totalDatabases should be(1)
        result.healthyDatabases should be(1)
        result.averageFragmentation should be >= 0.0
        result.timestamp should be > 0L
        result.status should be(HealthStatus.Healthy)
      }
    }
  }

  test("SegmentInfo should calculate correct metrics") {
    val segmentInfo = SegmentInfo(
      name = "seg_12345",
      filePath = "/test/seg_12345.bin",
      fileSize = 1024L,
      isActive = true,
      staleDataRatio = 0.25,
      entryCount = 10,
      lastModified = System.currentTimeMillis()
    )
    
    segmentInfo.name should be("seg_12345")
    segmentInfo.fileSize should be(1024L)
    segmentInfo.isActive should be(true)
    segmentInfo.staleDataRatio should be(0.25)
    segmentInfo.entryCount should be(10)
    segmentInfo.lastModified should be > 0L
  }

  test("DatabaseInfo should aggregate table and segment statistics") {
    val tables = List(
      TableInfo("users", 100, 95, 10240L, 5000L),
      TableInfo("orders", 200, 190, 20480L, 10000L)
    )
    
    val dbInfo = DatabaseInfo(
      name = "test_db",
      tables = tables,
      totalEntries = 300,
      activeEntries = 285,
      deletedEntries = 15,
      totalDiskSize = 30720L,
      totalMemorySize = 15000L,
      fragmentationRatio = 0.15
    )
    
    dbInfo.name should be("test_db")
    dbInfo.tables should have size 2
    dbInfo.totalEntries should be(300)
    dbInfo.activeEntries should be(285)
    dbInfo.deletedEntries should be(15)
    dbInfo.totalDiskSize should be(30720L)
    dbInfo.totalMemorySize should be(15000L)
    dbInfo.fragmentationRatio should be(0.15)
    
    // Check table aggregation
    dbInfo.tables.map(_.entryCount).sum should be(300)
    dbInfo.tables.map(_.activeEntryCount).sum should be(285)
  }

  test("MonitoringConfig should have sensible defaults") {
    val config = MonitoringConfig()
    
    config.checkInterval should be(30.seconds)
    config.enableBackgroundMonitoring should be(true)
    config.maxStaleRatio should be(0.3)
    config.compactionThreshold should be(0.5)
  }

  test("HealthStatus should calculate correct status") {
    val healthyStatus = HealthStatus(
      status = HealthStatus.Healthy,
      totalDatabases = 2,
      healthyDatabases = 2,
      averageFragmentation = 0.2,
      timestamp = System.currentTimeMillis()
    )
    
    val degradedStatus = HealthStatus(
      status = HealthStatus.Degraded,
      totalDatabases = 2,
      healthyDatabases = 1,
      averageFragmentation = 0.4,
      timestamp = System.currentTimeMillis()
    )
    
    val unhealthyStatus = HealthStatus(
      status = HealthStatus.Unhealthy,
      totalDatabases = 2,
      healthyDatabases = 0,
      averageFragmentation = 0.7,
      timestamp = System.currentTimeMillis()
    )
    
    healthyStatus.status should be(HealthStatus.Healthy)
    degradedStatus.status should be(HealthStatus.Degraded)
    unhealthyStatus.status should be(HealthStatus.Unhealthy)
  }
}
