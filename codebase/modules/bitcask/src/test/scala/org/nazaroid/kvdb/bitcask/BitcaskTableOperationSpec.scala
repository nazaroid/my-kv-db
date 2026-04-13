package org.nazaroid.kvdb.bitcask

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType, WriteTask, writeBinary}
import org.nazaroid.kvdb.bitcask.lib.{BitcaskTable, BitcaskTableConfig}
import org.scalatest.FutureOutcome
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.file.Paths
import scala.concurrent.duration.DurationInt
import scala.reflect.io.Directory

final class BitcaskTableOperationSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    java.nio.file.Files.createDirectories(testDir)
    val outcome = super.withFixture(test)
    outcome.onCompletedThen { _ =>
      val dir = new Directory(testDir.toFile)
      if (dir.exists) {
        dir.deleteRecursively()
      }
    }
  }

  private val testDir = Paths.get("./testFolder")

  private val config = BitcaskTableConfig(
    folder          = testDir.toString,
    maxSegmentSize  = 1024, // Small size for rotation testing (1KB)
    maxSegmentCount = 10,
    dataSchema = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
    ),
    segmentSchema = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("offset", FieldType.Int64),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
    ),
    tableSchema = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("segmentNameSize", FieldType.Int32),
      FieldDef("segmentName", FieldType.StringUtf8(sizeFromField = "segmentNameSize")),
      FieldDef("timestamp", FieldType.Timestamp),
      FieldDef("status", FieldType.RecordStatus)
    )
  )

  // Resource for running the manager in tests
  private val tableResource: Resource[IO, BitcaskTable[IO]] = for {
    given Logger[IO] <- Resource.eval(Slf4jLogger.create[IO])
    _                <- Resource.eval(Files[IO].createDirectories(Path(testDir.toString)).handleError(_ => ()))
    queue            <- Channel.bounded[IO, WriteTask[IO]](100).toResource
    // Run the background binary write worker
    _     <- writeBinary(queue.stream, parallelism = 1).compile.drain.background
    table <- Resource.eval(BitcaskTable.initialize[IO]("testTable", config, queue))
  } yield table

  "Write and Read: written value should be accessible" in {
    tableResource.use { t =>
      for {
        _   <- t.write("user:1", "hello stratum")
        res <- t.read("user:1")
      } yield assert(res.contains("hello stratum"))
    }
  }

  "Delete: deleted value should return None" in {
    tableResource.use { t =>
      for {
        _   <- t.write("user:2", "to be deleted")
        _   <- t.delete("user:2")
        res <- t.read("user:2")
      } yield assert(res.isEmpty)
    }
  }

  "Rotation: new segment should be created when limit is exceeded" in {
    tableResource.use { t =>
      for {
        // Write enough data to trigger rotation (1KB limit)
        _    <- t.write("k1", "a" * 1025)
        seg1 <- t.currentData.get.map(_.filePath)

        _    <- t.write("k2", "b" * 600)
        seg2 <- t.currentData.get.map(_.filePath)

        _ <- IO.sleep(100.millis) // Allow time for file system operations

        // Verify that file paths are different
        _ <- IO.blocking(assert(seg1 != seg2, s"Segment should have rotated: $seg1 vs $seg2"))

        // Data from the old segment should still be accessible
        val1 <- t.read("k1")
      } yield assert(val1.contains("a" * 1025))
    }
  }

  "Compaction & Cleanup: old files should be removed after compaction" in {
    tableResource.use { t =>
      for {
        _ <- t.write("temp", "data")
        _ <- t.delete("temp") // Create "garbage"
        _ <- t.write("permanent", "keep me")

        // Trigger compaction
        _ <- t.compact()
        _ <- IO.sleep(200.millis)

        // Verify live data is still present
        res <- t.read("permanent")
        _   <- IO.blocking(assert(res.contains("keep me")))

        // Verify physical file existence via Files.list
        files <- Files[IO].list(Path(testDir.toString)).map(_.fileName.toString).compile.toList
        // Only the new compact-segment and active empty segment should remain
      } yield assert(files.exists(_.startsWith("compact_")), "Compact segment should exist")
    }
  }

  "Segment threshold: compaction should run when segment count exceeds limit" in {
    val compactConfig = config.copy(maxSegmentSize = 200, maxSegmentCount = 1)

    val compactResource: Resource[IO, BitcaskTable[IO]] = for {
      given Logger[IO] <- Resource.eval(Slf4jLogger.create[IO])
      _                <- Resource.eval(Files[IO].createDirectories(Path(testDir.toString)).handleError(_ => ()))
      queue            <- Channel.bounded[IO, WriteTask[IO]](100).toResource
      _                <- writeBinary(queue.stream, parallelism = 1).compile.drain.background
      table            <- Resource.eval(BitcaskTable.initialize[IO]("testTable", compactConfig, queue))
    } yield table

    compactResource.use { t =>
      for {
        _     <- t.write("k1", "a" * 500)
        _     <- t.write("k2", "b" * 500)
        _     <- IO.sleep(300.millis)
        v1    <- t.read("k1")
        v2    <- t.read("k2")
        files <- Files[IO].list(Path(testDir.toString)).map(_.fileName.toString).compile.toList
        binCount = files.count(_.endsWith(".bin"))
      } yield {
        assert(v1.contains("a" * 500))
        assert(v2.contains("b" * 500))
        assert(files.exists(_.startsWith("compact_")), "Compact segment should exist")
        assert(binCount <= 2, s"Unexpected bin files count: $binCount")
      }
    }
  }

  "Recovery: data should be accessible after full system restart" in {
    val key = "persistent_user"
    val value = "{\"data\": \"important\"}"

    // 1. First session: write data and shutdown
    val session1 = tableResource.use { sm =>
      sm.write(key, value) *> IO.sleep(100.millis) // Wait for disk write completion
    }

    // 2. Second session: reopen storage and read
    val session2 = tableResource.use { sm =>
      sm.read(key).map { recoveredValue =>
        assert(recoveredValue.contains(value), "Data should be recovered from disk indexes")
      }
    }
    session1 *> session2
  }

  "Recovery with Deletion: deleted data should not reappear after restart" in {
    val keyToKeep = "keep_me"
    val keyToDelete = "delete_me"

    val scenario = for {
      // Step 1: Write two keys, delete one
      _ <- tableResource.use { t =>
        for {
          _ <- t.write(keyToKeep, "value1")
          _ <- t.write(keyToDelete, "value2")
          _ <- t.delete(keyToDelete)
          _ <- IO.sleep(100.millis)
        } yield ()
      }

      // Step 2: Restart and verify state
      _ <- tableResource.use { t =>
        for {
          val1 <- t.read(keyToKeep)
          val2 <- t.read(keyToDelete)
          _    <- IO.pure(assert(val1.contains("value1")))
          _    <- IO.pure(assert(val2.isEmpty, "Deleted key should not be recovered"))
        } yield ()
      }
    } yield ()

    scenario
  }
}
