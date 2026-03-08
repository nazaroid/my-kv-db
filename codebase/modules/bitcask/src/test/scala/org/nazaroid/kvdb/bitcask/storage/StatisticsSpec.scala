package org.nazaroid.kvdb.bitcask.storage

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.nazaroid.kvdb.bitcask.storage.{StorageConfig, StorageManager, Statistics}
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import java.nio.file.{Files => JFiles, Paths}

class StatisticsSpec extends AnyFunSuite with Matchers {

  def withTempDirectory[T](test: Path => IO[T]): IO[T] = {
    IO.delay {
      val tempDir = JFiles.createTempDirectory("kvdb-stats-test")
      Path.fromNioPath(tempDir.toAbsolutePath)
    }.bracket(dir => IO.delay {
      // Clean up
      JFiles.walk(dir.toNioPath)
        .filter(_.toFile.isFile)
        .forEach(JFiles.deleteIfExists)
      JFiles.deleteIfExists(dir.toNioPath)
    })
  }

  test("getStats should return correct database statistics") {
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
      
      val config = StorageConfig(
        folder = tempDir.toString,
        maxSegmentSize = 1024,
        maxSegmentCount = 10,
        dataSchema = dataSchema,
        segmentSchema = segmentSchema,
        tableSchema = tableSchema,
        maxRetries = 3
      )
      
      val result = (for {
        queue <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager <- StorageManager.initialize(config, queue)
        
        // Add test data
        _ <- storageManager.write("db1/table1/key1", "value1")
        _ <- storageManager.write("db1/table1/key2", "value2")
        _ <- storageManager.write("db1/table2/key1", "value3")
        _ <- storageManager.write("db1/table2/key2", "value4")
        _ <- storageManager.write("db1/table1/key3", "value5")
        _ <- storageManager.delete("db1/table1/key3") // Delete one entry
        
        stats <- storageManager.getStats
      } yield stats).unsafeRunSync()
      
      result.totalTables should be(2)
      result.totalEntries should be(4) // 5 entries - 1 deleted
      result.activeEntries should be(4)
      result.deletedEntries should be(1)
      result.totalDataSize should be > 0L
      result.tableStats.size should be(2)
      
      // Check table-specific stats
      val table1Stats = result.tableStats.find(_.name == "db1/table1")
      val table2Stats = result.tableStats.find(_.name == "db1/table2")
      
      table1Stats should be(defined)
      table1Stats.get.entryCount should be(2) // key1, key2 (key3 deleted)
      table1Stats.get.activeEntryCount should be(2)
      
      table2Stats should be(defined)
      table2Stats.get.entryCount should be(2) // key1, key2
      table2Stats.get.activeEntryCount should be(2)
      
      result.segmentStats.size should be >= 1
    }
  }

  test("exportStatsForPrometheus should generate valid Prometheus format") {
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
      
      val config = StorageConfig(
        folder = tempDir.toString,
        maxSegmentSize = 1024,
        maxSegmentCount = 10,
        dataSchema = dataSchema,
        segmentSchema = segmentSchema,
        tableSchema = tableSchema,
        maxRetries = 3
      )
      
      val result = (for {
        queue <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager <- StorageManager.initialize(config, queue)
        
        // Add test data
        _ <- storageManager.write("test_db/test_table/key1", "value1")
        _ <- storageManager.write("test_db/test_table/key2", "value2")
        
        prometheusExport <- storageManager.exportStatsForPrometheus
      } yield prometheusExport).unsafeRunSync()
      
      // Check Prometheus format
      result should include("# HELP kvdb_database_stats")
      result should include("# TYPE kvdb_database_stats gauge")
      result should include("kvdb_database_stats{type=\"total\"} 1")
      result should include("kvdb_database_stats{type=\"entries\"} 2")
      result should include("kvdb_database_stats{type=\"active_entries\"} 2")
      result should include("kvdb_database_stats{type=\"deleted_entries\"} 0")
      result should include("kvdb_database_stats{type=\"data_size_bytes\"}")
      result should include("# HELP kvdb_table_stats")
      result should include("# TYPE kvdb_table_stats gauge")
      result should include("kvdb_table_stats{table=\"test_db/test_table\"} 2")
      result should include("kvdb_table_stats{table=\"test_db/test_table\",type=\"active_entries\"} 2")
      result should include("# HELP kvdb_segment_stats")
      result should include("# TYPE kvdb_segment_stats gauge")
    }
  }

  test("segment statistics should calculate correct metrics") {
    val segmentStats = List(
      SegmentStats("seg_12345", 1024L, true, 0.1),
      SegmentStats("seg_67890", 2048L, false, 0.3),
      SegmentStats("seg_11111", 512L, true, 0.05)
    )
    
    // Test segment stats calculations
    segmentStats.size should be(3)
    segmentStats.count(_.isActive) should be(2)
    segmentStats.map(_.fileSize).sum should be(3584L)
    segmentStats.map(_.staleDataRatio).sum should be(0.45)
  }

  test("table statistics should aggregate correctly") {
    val tableStats = List(
      TableStats("users", 100, 95),
      TableStats("orders", 250, 240),
      TableStats("products", 50, 45)
    )
    
    val totalEntries = tableStats.map(_.entryCount).sum
    val totalActive = tableStats.map(_.activeEntryCount).sum
    
    totalEntries should be(400)
    totalActive should be(380)
    
    // Calculate deleted entries
    val totalDeleted = tableStats.map(t => t.entryCount - t.activeEntryCount).sum
    totalDeleted should be(20)
  }

  test("database statistics should combine all metrics correctly") {
    val dbStats = DatabaseStats(
      totalTables = 5,
      totalEntries = 1000,
      activeEntries = 950,
      deletedEntries = 50,
      totalDataSize = 1048576L,
      tableStats = List(
        TableStats("table1", 200, 190),
        TableStats("table2", 300, 285),
        TableStats("table3", 150, 140),
        TableStats("table4", 100, 95),
        TableStats("table5", 250, 240)
      ),
      segmentStats = List(
        SegmentStats("seg_1", 1024L, true, 0.1),
        SegmentStats("seg_2", 2048L, true, 0.15),
        SegmentStats("seg_3", 512L, false, 0.4)
      )
    )
    
    dbStats.totalTables should be(5)
    dbStats.totalEntries should be(1000)
    dbStats.activeEntries should be(950)
    dbStats.deletedEntries should be(50)
    dbStats.totalDataSize should be(1048576L)
    dbStats.tableStats.size should be(5)
    dbStats.tableStats.map(_.entryCount).sum should be(1000)
    dbStats.tableStats.map(_.activeEntryCount).sum should be(950)
    dbStats.segmentStats.size should be(3)
    dbStats.segmentStats.count(_.isActive) should be(2)
    dbStats.segmentStats.map(_.fileSize).sum should be(3584L)
  }
}
