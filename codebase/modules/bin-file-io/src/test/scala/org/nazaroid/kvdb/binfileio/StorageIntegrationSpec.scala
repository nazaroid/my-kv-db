package org.nazaroid.kvdb.binfileio

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import cats.implicits.given
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.bitcask.storage.{StorageConfig, StorageManager}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

import java.nio.file.{Files => JFiles, Paths}

class StorageIntegrationSpec extends AnyFunSuite with Matchers {

  def withTempDirectory[T](test: Path => IO[T]): IO[T] = {
    IO.delay {
      val tempDir = JFiles.createTempDirectory("kvdb-test")
      Path.fromNioPath(tempDir.toAbsolutePath)
    }.bracket(dir => IO.delay {
      // Clean up
      JFiles.walk(dir.toNioPath)
        .filter(_.toFile.isFile)
        .forEach(JFiles.deleteIfExists)
      JFiles.deleteIfExists(dir.toNioPath)
    })
  }

  test("StorageManager should handle CRC-enabled data files correctly") {
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
        _ <- storageManager.write("testKey", "testValue")
        readValue <- storageManager.read("testKey")
      } yield readValue).unsafeRunSync()
      
      result shouldBe Some("testValue")
    }
  }

  test("StorageManager should handle corrupted data gracefully") {
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
        
        // Create corrupted data file manually
        val dataFile = tempDir / "seg_0.bin"
        val validRow = Map(
          "recordSize" -> 9,
          "value" -> "testValue",
          "timestamp" -> System.currentTimeMillis(),
          "status" -> 1,
          "crc" -> 0L
        )
        val validEncoded = encode(validRow, dataSchema)
        
        // Write corrupted data (wrong CRC)
        val corruptedData = validEncoded.toByteVector.dropRight(8) ++ ByteVector.fromLong(99999L)
        Files[IO].writeAll(dataFile).compile.drain
        
        // Try to read corrupted data
        readValue <- storageManager.read("testKey")
      } yield readValue).unsafeRunSync()
      
      // Should return None due to CRC validation failure
      result shouldBe None
    }
  }

  test("StorageManager should handle segment and table files without CRC") {
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
        _ <- storageManager.write("testKey", "testValue")
        readValue <- storageManager.read("testKey")
      } yield readValue).unsafeRunSync()
      
      result shouldBe Some("testValue")
    }
  }

  test("StorageManager should handle write failures with retry logic") {
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
        
        // Make directory read-only to simulate write failure
        val dataFile = tempDir / "seg_0.bin"
        _ <- Files[IO].setPosixPermissions(dataFile, java.nio.file.attribute.PosixFilePermissions.fromString("r--r--r--"))
        
        writeResult <- storageManager.write("testKey", "testValue")
      } yield writeResult).unsafeRunSync()
      
      // Should fail due to permission error
      writeResult shouldBe a[Left[String]]
      writeResult match {
        case Left(error) => error should include("Write failed")
        case Right(_) => fail("Should not succeed with read-only directory")
      }
    }
  }

  test("StorageManager should handle delete operations correctly") {
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
        _ <- storageManager.write("testKey", "testValue")
        readValue1 <- storageManager.read("testKey")
        _ <- storageManager.delete("testKey")
        readValue2 <- storageManager.read("testKey")
      } yield (readValue1, readValue2)).unsafeRunSync()
      
      readValue1 shouldBe Some("testValue")
      readValue2 shouldBe None // Should be None after delete
    }
  }
}
