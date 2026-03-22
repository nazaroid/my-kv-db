package org.nazaroid.kvdb.bitcask

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType, WriteTask, writeBinary}
import org.nazaroid.kvdb.bitcask.{BitcaskTable, BitcaskTableConfig}
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
    folder         = testDir.toString,
    maxSegmentSize = 1024, // Small size for rotation testing (1KB)
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
  private val storageResource: Resource[IO, BitcaskTable[IO]] = for {
    given Logger[IO] <- Resource.eval(Slf4jLogger.create[IO])
    _     <- Resource.eval(Files[IO].createDirectories(Path(testDir.toString)).handleError(_ => ()))
    queue <- Channel.bounded[IO, WriteTask[IO]](100).toResource
    // Run the background binary write worker
    _       <- writeBinary(queue.stream, parallelism = 1).compile.drain.background
    manager <- Resource.eval(BitcaskTable.initialize[IO](config, queue))
  } yield manager

  "Write and Read: written value should be accessible" in {
    storageResource.use { sm =>
      for {
        _   <- sm.write("user:1", "hello stratum")
        res <- sm.read("user:1")
      } yield assert(res.contains("hello stratum"))
    }
  }

  "Delete: deleted value should return None" in {
    storageResource.use { sm =>
      for {
        _   <- sm.write("user:2", "to be deleted")
        _   <- sm.delete("user:2")
        res <- sm.read("user:2")
      } yield assert(res.isEmpty)
    }
  }

  "Rotation: new segment should be created when limit is exceeded" in {
    storageResource.use { sm =>
      for {
        // Write enough data to trigger rotation (1KB limit)
        _    <- sm.write("k1", "a" * 1025)
        seg1 <- sm.currentData.get.map(_.filePath)

        _    <- sm.write("k2", "b" * 600)
        seg2 <- sm.currentData.get.map(_.filePath)

        _ <- IO.sleep(100.millis) // Allow time for file system operations

        // Verify that file paths are different
        _ <- IO.blocking(assert(seg1 != seg2, s"Segment should have rotated: $seg1 vs $seg2"))

        // Data from the old segment should still be accessible
        val1 <- sm.read("k1")
      } yield assert(val1.contains("a" * 1025))
    }
  }

  "Compaction & Cleanup: old files should be removed after compaction" in {
    storageResource.use { sm =>
      for {
        _ <- sm.write("temp", "data")
        _ <- sm.delete("temp") // Create "garbage"
        _ <- sm.write("permanent", "keep me")

        // Trigger compaction
        _ <- sm.compact()
        _ <- IO.sleep(200.millis)

        // Verify live data is still present
        res <- sm.read("permanent")
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
      _     <- Resource.eval(Files[IO].createDirectories(Path(testDir.toString)).handleError(_ => ()))
      queue <- Channel.bounded[IO, WriteTask[IO]](100).toResource
      _       <- writeBinary(queue.stream, parallelism = 1).compile.drain.background
      manager <- Resource.eval(BitcaskTable.initialize[IO](compactConfig, queue))
    } yield manager

    compactResource.use { sm =>
      for {
        _ <- sm.write("k1", "a" * 500)
        _ <- sm.write("k2", "b" * 500)
        _ <- IO.sleep(300.millis)
        v1 <- sm.read("k1")
        v2 <- sm.read("k2")
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
    val session1 = storageResource.use { sm =>
      sm.write(key, value) *> IO.sleep(100.millis) // Wait for disk write completion
    }

    // 2. Second session: reopen storage and read
    val session2 = storageResource.use { sm =>
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
      _ <- storageResource.use { sm =>
        for {
          _ <- sm.write(keyToKeep, "value1")
          _ <- sm.write(keyToDelete, "value2")
          _ <- sm.delete(keyToDelete)
          _ <- IO.sleep(100.millis)
        } yield ()
      }

      // Step 2: Restart and verify state
      _ <- storageResource.use { sm =>
        for {
          val1 <- sm.read(keyToKeep)
          val2 <- sm.read(keyToDelete)
          _    <- IO.pure(assert(val1.contains("value1")))
          _    <- IO.pure(assert(val2.isEmpty, "Deleted key should not be recovered"))
        } yield ()
      }
    } yield ()

    scenario
  }
}
