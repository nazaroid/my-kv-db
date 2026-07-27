package org.nazaroid.kvdb.bitcask

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import fs2.{Chunk, Stream}
import org.nazaroid.kvdb.binfileio.*
import org.nazaroid.kvdb.bitcask.lib.{BitcaskTable, BitcaskTableConfig}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scodec.bits.ByteVector

import java.nio.file.Files as JFiles
import scala.concurrent.duration.DurationInt

final class BitcaskTableErrorHandlingSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

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

  "BitcaskTable should handle CRC-enabled data files correctly" in {
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
        table            <- BitcaskTable.initialize("testTable", config, queue)
        (_, release)     <- writeBinary[IO](queue.stream, parallelism = 1).compile.drain.background.allocated
        _                <- table.write("testKey", "testValue")
        readValue        <- table.read("testKey")
        _                <- release
      } yield readValue shouldBe Some("testValue")
    }
  }

  "BitcaskTable should handle corrupted data gracefully" in {
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
        table            <- BitcaskTable.initialize("testTable", config, queue)
        (_, release)     <- writeBinary[IO](queue.stream, parallelism = 1).compile.drain.background.allocated

        // Create corrupted data file manually
        dataFile = tempDir / "seg_0.bin"
        validRow = Map(
          "valueSize" -> 9,
          "value"     -> "testValue",
          "timestamp" -> System.currentTimeMillis(),
          "status"    -> 1,
          "crc"       -> 0L
        )
        validEncoded = encode(validRow, dataSchema).fold(err => fail(err), identity)

        // Write corrupted data (wrong CRC)
        corruptedData = validEncoded.toByteVector.dropRight(8) ++ ByteVector.fromLong(99999L)
        _ <- Stream.emits(corruptedData.toArray).through(Files[IO].writeAll(dataFile)).compile.drain
        // Try to read corrupted data
        readValue <- table.read("testKey")
        _         <- release
        // Should return None due to CRC validation failure
      } yield readValue shouldBe None
    }
  }

  "BitcaskTable should handle segment and table files without CRC" in {
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
        table            <- BitcaskTable.initialize("testTable", config, queue)
        (_, release)     <- writeBinary[IO](queue.stream, parallelism = 1).compile.drain.background.allocated
        _                <- table.write("testKey", "testValue")
        readValue        <- table.read("testKey")
        _                <- release
      } yield readValue shouldBe Some("testValue")
    }
  }

  "BitcaskTable should handle write failures with retry logic" in {
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
        table            <- BitcaskTable.initialize("testTable", config, queue)
        (_, release)     <- writeBinary[IO](queue.stream, parallelism = 1).compile.drain.background.allocated
        writeResult <- table.write("testKey1", "testValue1")
        _ <- IO.sleep(300.millis)
        dataFile <- Files[IO].list(tempDir, "seg_*.bin").compile.lastOrError
        // Make directory read-only to simulate write failure
        _ <- Files[IO].setPosixPermissions(
          dataFile,
          fs2.io.file.PosixPermissions.fromString("r--r--r--").get
        )
        writeResult <- table.write("testKey2", "testValue2")
        _           <- release
      } yield {
        // Should fail due to permission error
        writeResult shouldBe a[Left[String, Row]]
        writeResult match {
          case Left(error) => error should include("Write operation failed")
          case Right(_)    => fail("Should not succeed with read-only directory")
        }
      }
    }
  }

  "BitcaskTable should handle delete operations correctly" in {
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
        table            <- BitcaskTable.initialize("testTable", config, queue)
        (_, release)     <- writeBinary[IO](queue.stream, parallelism = 1).compile.drain.background.allocated
        _                <- table.write("testKey", "testValue")
        readValue1       <- table.read("testKey")
        _                <- table.delete("testKey")
        readValue2       <- table.read("testKey")
        _                <- release
      } yield {
        readValue1 shouldBe Some("testValue")
        readValue2 shouldBe None // Should be None after delete
      }
    }
  }
}
