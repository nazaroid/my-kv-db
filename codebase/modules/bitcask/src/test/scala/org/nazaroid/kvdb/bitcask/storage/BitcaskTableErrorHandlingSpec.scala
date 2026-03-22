package org.nazaroid.kvdb.bitcask.storage

import cats.effect.IO
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import fs2.{Chunk, Stream}
import org.nazaroid.kvdb.binfileio.*
import org.nazaroid.kvdb.bitcask.storage.BitcaskTable
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scodec.bits.ByteVector

import java.nio.file.Files as JFiles

final class BitcaskTableErrorHandlingSpec extends AnyFunSuite with Matchers {

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

  test("StorageManager should handle CRC-enabled data files correctly") {
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

      val config = BitcaskTableConfig(
        folder          = tempDir.toString,
        maxSegmentSize  = 1024,
        maxSegmentCount = 10,
        dataSchema      = dataSchema,
        segmentSchema   = segmentSchema,
        tableSchema     = tableSchema,
        maxRetries      = 3
      )

      for {
        given Logger[IO] <- Slf4jLogger.create[IO]
        queue            <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager   <- BitcaskTable.initialize(config, queue)
        _                <- storageManager.write("testKey", "testValue")
        readValue        <- storageManager.read("testKey")

      } yield readValue shouldBe Some("testValue")
    }
  }

  test("StorageManager should handle corrupted data gracefully") {
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

      val config = BitcaskTableConfig(
        folder          = tempDir.toString,
        maxSegmentSize  = 1024,
        maxSegmentCount = 10,
        dataSchema      = dataSchema,
        segmentSchema   = segmentSchema,
        tableSchema     = tableSchema,
        maxRetries      = 3
      )

      for {
        given Logger[IO] <- Slf4jLogger.create[IO]
        queue            <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager   <- BitcaskTable.initialize(config, queue)

        // Create corrupted data file manually
        dataFile = tempDir / "seg_0.bin"
        validRow = Map(
          "valueSize" -> 9,
          "value"      -> "testValue",
          "timestamp"  -> System.currentTimeMillis(),
          "status"     -> 1,
          "crc"        -> 0L
        )
        validEncoded = encode(validRow, dataSchema)

        // Write corrupted data (wrong CRC)
        corruptedData = validEncoded.toByteVector.dropRight(8) ++ ByteVector.fromLong(99999L)
        _ <- Stream.emits(corruptedData.toArray).through(Files[IO].writeAll(dataFile)).compile.drain
        // Try to read corrupted data
        readValue <- storageManager.read("testKey")
        // Should return None due to CRC validation failure
      } yield readValue shouldBe None
    }
  }

  test("StorageManager should handle segment and table files without CRC") {
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

      val config = BitcaskTableConfig(
        folder          = tempDir.toString,
        maxSegmentSize  = 1024,
        maxSegmentCount = 10,
        dataSchema      = dataSchema,
        segmentSchema   = segmentSchema,
        tableSchema     = tableSchema,
        maxRetries      = 3
      )

      for {
        given Logger[IO] <- Slf4jLogger.create[IO]
        queue            <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager   <- BitcaskTable.initialize(config, queue)
        _                <- storageManager.write("testKey", "testValue")
        readValue        <- storageManager.read("testKey")
      } yield readValue shouldBe Some("testValue")
    }
  }

  test("StorageManager should handle write failures with retry logic") {
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

      val config = BitcaskTableConfig(
        folder          = tempDir.toString,
        maxSegmentSize  = 1024,
        maxSegmentCount = 10,
        dataSchema      = dataSchema,
        segmentSchema   = segmentSchema,
        tableSchema     = tableSchema,
        maxRetries      = 3
      )

      for {
        given Logger[IO] <- Slf4jLogger.create[IO]
        queue            <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager   <- BitcaskTable.initialize(config, queue)
        // Make directory read-only to simulate write failure
        dataFile = tempDir / "seg_0.bin"
        _ <- Files[IO].setPosixPermissions(
          dataFile,
          fs2.io.file.PosixPermissions.fromString("r--r--r--").get
        )
        writeResult <- storageManager.write("testKey", "testValue")
      } yield {
        // Should fail due to permission error
        writeResult shouldBe a[Left[String, Row]]
        writeResult match {
          case Left(error) => error should include("Write failed")
          case Right(_)    => fail("Should not succeed with read-only directory")
        }
      }
    }
  }

  test("StorageManager should handle delete operations correctly") {
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

      val config = BitcaskTableConfig(
        folder          = tempDir.toString,
        maxSegmentSize  = 1024,
        maxSegmentCount = 10,
        dataSchema      = dataSchema,
        segmentSchema   = segmentSchema,
        tableSchema     = tableSchema,
        maxRetries      = 3
      )

      for {
        given Logger[IO] <- Slf4jLogger.create[IO]
        queue            <- Channel.unbounded[IO, WriteTask[IO]]
        storageManager   <- BitcaskTable.initialize(config, queue)
        _                <- storageManager.write("testKey", "testValue")
        readValue1       <- storageManager.read("testKey")
        _                <- storageManager.delete("testKey")
        readValue2       <- storageManager.read("testKey")
      } yield {
        readValue1 shouldBe Some("testValue")
        readValue2 shouldBe None // Should be None after delete
      }
    }
  }
}
