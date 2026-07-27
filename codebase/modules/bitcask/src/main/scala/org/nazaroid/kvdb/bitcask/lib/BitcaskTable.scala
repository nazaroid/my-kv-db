package org.nazaroid.kvdb.bitcask.lib

import cats.effect.{Async, Deferred, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.{Files, Flag, Flags, Path}
import org.nazaroid.kvdb.binfileio.*
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.*

enum CacheEntry {
  case Pending(row: Row)

  case Persistent(
    row:     Row,
    segment: String,
    offset:  Long)
  case Deleted
}

case class BitcaskTableConfig(
  folder:          String,
  maxSegmentSize:  Long,
  maxSegmentCount: Int,
  dataSchema:      List[FieldDef],
  segmentSchema:   List[FieldDef],
  tableSchema:     List[FieldDef],
  maxRetries:      Int = 3)

class BinFileStorage[F[_]: Async: Files](
  val filePath: String,
  val schema:   List[FieldDef],
  val queue:    Channel[F, WriteTask[F]],
  private var size: Long = 0) {

  def currentSize: Long = size

  def append(id: String, row: Row): F[Long] =
    for {
      encoded <- Async[F].fromEither(encode(row, schema).leftMap(new IllegalArgumentException(_)))
      promise <- Deferred[F, Long]
      recordSize = 4 + encoded.size
      _       <- queue.send(WriteTask(id, filePath, schema, row, Some(promise)))
      offset  <- promise.get
      _ <- Async[F].delay {
        if (offset >= 0) size = offset + recordSize
      }
    } yield offset
}

sealed class BitcaskTable[F[_]: Async: Files: Logger](
  val name:              String,
  val currentData:       Ref[F, BinFileStorage[F]],
  val currentSegmentIdx: Ref[F, BinFileStorage[F]],
  val tableStorage:      BinFileStorage[F],
  val cache:             Ref[F, Map[String, CacheEntry]],
  val config:            BitcaskTableConfig,
  val writeQueue:        Channel[F, WriteTask[F]]) {

  /** WRITE: Data -> Segment -> Table -> Cache */
  def write(key: String, value: String): F[Either[String, Unit]] = {
    val timestamp = System.currentTimeMillis()
    val status = 1 // Active status

    val dataRow = Map(
      "valueSize" -> utf8ByteLength(value),
      "value"     -> value,
      "timestamp" -> timestamp,
      "status"    -> status,
      "crc"       -> 0L // CRC field will be filled during encoding
    )

    for {
      _ <- cache.update(_ + (key -> CacheEntry.Pending(dataRow)))

      ds <- currentData.get

      rotation <- if (ds.currentSize > config.maxSegmentSize) rotate().map(_ -> true) else Async[F].pure((ds, false))
      (activeDS, rotated) = rotation
      activeSS <- currentSegmentIdx.get

      // Cascading write operations
      writeResult <- attemptWriteWithRetry(activeDS, key, dataRow)
      offset <- writeResult match {
        case Right(off)  => Async[F].pure(off)
        case Left(error) =>
          // If there is a write error, mark it as failed and continue
          Logger[F].warn(s"Failed to write data for key $key: $error") *>
            Async[F].pure(-1L) // Use -1 as an error indicator
      }

      // Continue writing the segment and table only if the data is written successfully
      segRow = Map(
        "keySize"   -> utf8ByteLength(key),
        "key"       -> key,
        "offset"    -> offset,
        "timestamp" -> timestamp,
        "status"    -> status
      )

      segWriteResult <- attemptWriteWithRetry(activeSS, key, segRow)
      segOffset <- segWriteResult match {
        case Right(off) => Async[F].pure(off)
        case Left(error) =>
          Logger[F].warn(s"Failed to write segment for key $key: $error") *>
            Async[F].pure(-1L)
      }

      segName = Path(activeDS.filePath).fileName.toString.replace(".bin", "")
      tableRow = Map(
        "keySize"         -> utf8ByteLength(key),
        "key"             -> key,
        "segmentNameSize" -> utf8ByteLength(segName),
        "segmentName"     -> segName,
        "timestamp"       -> timestamp,
        "status"          -> status
      )

      tableWriteResult <- attemptWriteWithRetry(tableStorage, key, tableRow)
      tableOffset <- tableWriteResult match {
        case Right(off) => Async[F].pure(off)
        case Left(error) =>
          Logger[F].warn(s"Failed to write table for key $key: $error") *>
            Async[F].pure(-1L)
      }

      // Update the cache only if all operations are successful
      finalResult <-
        if (offset >= 0 && segOffset >= 0 && tableOffset >= 0) {
          cache.update(_ + (key -> CacheEntry.Persistent(dataRow, segName, offset))).map(_ => Right(()))
        } else {
          cache.update(_ - key).map(_ => Left("Write operation failed"))
        }
      _ <- compactIfNeeded
    } yield finalResult
  }

  /** READ: Always from cache (O(1)) */
  def read(key: String): F[Option[String]] = {
    cache
      .get
      .map(_.get(key).flatMap {
        case CacheEntry.Pending(row) => // Check record status
          row.get("status") match {
            case Some(1) => row.get("value").map(_.toString) // Active
            case _       => None                             // Not active
          }
        case CacheEntry.Persistent(row, _, _) =>
          row.get("status") match {
            case Some(1) => row.get("value").map(_.toString) // Active
            case _       => None                             // Not active
          }
        case CacheEntry.Deleted => None
      })
  }

  /** Helper method for retry logic */
  private def attemptWriteWithRetry(
    storage: BinFileStorage[F],
    key:     String,
    row:     Row
  ): F[Either[String, Long]] = {
    def attemptWrite(attempt: Int): F[Either[String, Long]] = {
      storage.append(key, row).attempt.flatMap {
        case Right(offset) => Async[F].pure(Right(offset))
        case Left(error) =>
          if (attempt < config.maxRetries) {
            Logger[F].warn(s"Write attempt $attempt failed for key $key, retrying...") *>
              Async[F].sleep(50.millis * attempt) *> attemptWrite(attempt + 1)
          } else {
            Logger[F].error(s"Write failed after $attempt attempts for key $key: $error") *>
              Async[F].pure(Left(s"Write failed: $error"))
          }
      }
    }
    attemptWrite(1)
  }

  /** DELETE: Write a Tombstone record */
  def delete(key: String): F[Unit] = {
    for {
      _ <- cache.update(_ + (key -> CacheEntry.Deleted))
      tRow = Map(
        "keySize"         -> utf8ByteLength(key),
        "key"             -> key,
        "segmentNameSize" -> 7,
        "segmentName"     -> "DELETED",
        "timestamp"       -> System.currentTimeMillis(),
        "status"          -> 0 // Deleted status
      )
      _ <- tableStorage.append(key, tRow)
    } yield ()
  }

  /** COMPACTION: Merge all segments into a single new one */
  def compact(): F[Unit] = {
    for {
      snapshot <- cache.get
      aliveEntries = snapshot.collect { case (key, CacheEntry.Persistent(row, _, _)) =>
        (key, row)
      }.toList

      _ <- Async[F].whenA(aliveEntries.nonEmpty) {
        for {
          compName <- Async[F].delay(s"compact_${System.currentTimeMillis()}")
          cDataPath = Path(s"${config.folder}/$compName.bin")
          cIdxPath = Path(s"${config.folder}/$compName.idx")

          newMappings <- aliveEntries.foldLeftM(Map.empty[String, CacheEntry]) { case (acc, (key, row)) =>
            for {
              bytes <- Async[F].fromEither(encode(row, config.dataSchema).leftMap(new IllegalArgumentException(_)))
              _ <- Stream
                .chunk(bytes)
                .through(Files[F].writeAll(cDataPath, Flags(Flag.Create, Flag.Append)))
                .compile
                .drain
              off <- Files[F].size(cDataPath).map(_ - bytes.size)

              iRow = Map(
                "keySize"   -> utf8ByteLength(key),
                "key"       -> key,
                "offset"    -> off,
                "timestamp" -> System.currentTimeMillis(),
                "status"    -> 1
              )
              iBytes <- Async[F].fromEither(encode(iRow, config.segmentSchema).leftMap(new IllegalArgumentException(_)))
              _ <- Stream
                .chunk(iBytes)
                .through(Files[F].writeAll(cIdxPath, Flags(Flag.Create, Flag.Append)))
                .compile
                .drain

              tRow = Map(
                "keySize"         -> utf8ByteLength(key),
                "key"             -> key,
                "segmentNameSize" -> utf8ByteLength(compName),
                "segmentName"     -> compName,
                "timestamp"       -> System.currentTimeMillis(),
                "status"          -> 1
              )
              _ <- tableStorage.append(key, tRow)
            } yield acc + (key -> CacheEntry.Persistent(row, compName, off))
          }

          _ <- cache.update(old => (old ++ newMappings).filter(_._2 != CacheEntry.Deleted))
          _ <- cleanup()
        } yield ()
      }
    } yield ()
  }

  /** CLEANUP: Physical deletion of orphaned files */
  def cleanup(): F[Unit] = {
    for {
      snapshot <- cache.get
      activeSegs = snapshot.collect { case (_, CacheEntry.Persistent(_, seg, _)) => seg }.toSet
      ds <- currentData.get
      current = Path(ds.filePath).fileName.toString.replace(".bin", "")

      files <- Files[F].list(Path(config.folder)).map(_.fileName.toString).compile.toList
      all = files.filter(_.endsWith(".bin")).map(_.replace(".bin", "")).toSet

      _ <- (all -- activeSegs - current).toList.traverse { seg =>
        Files[F].deleteIfExists(Path(s"${config.folder}/$seg.bin")) *>
          Files[F].deleteIfExists(Path(s"${config.folder}/$seg.idx"))
      }
    } yield ()
  }

  private def rotate(): F[BinFileStorage[F]] = {
    val name = s"seg_${System.currentTimeMillis()}"
    val nDS = new BinFileStorage(s"${config.folder}/$name.bin", config.dataSchema, writeQueue)
    val nSS = new BinFileStorage(s"${config.folder}/$name.idx", config.segmentSchema, writeQueue)
    currentData.set(nDS) *> currentSegmentIdx.set(nSS) *> Async[F].pure(nDS)
  }

  private def segmentCount: F[Int] =
    Files[F]
      .list(Path(config.folder))
      .map(_.fileName.toString)
      .filter(_.endsWith(".bin"))
      .compile
      .count
      .handleError(_ => 0L)
      .map(_.toInt)

  private def compactIfNeeded: F[Unit] =
    segmentCount.flatMap { count =>
      Async[F].whenA(count > config.maxSegmentCount)(compact())
    }

  /** Get segment statistics */
  private def getSegmentStats: F[List[SegmentStats]] = {
    for {
      files <- Files[F]
        .list(Path(config.folder))
        .map(_.fileName.toString)
        .filter(_.endsWith(".bin"))
        .compile
        .toList

      segmentStats <- files.traverse { fileName =>
        val segmentName = fileName.replace(".bin", "")
        val filePath = Path(s"${config.folder}/$fileName")

        for {
          fileSize <- Files[F].size(filePath)
          isActive <- currentData.get.map(_.filePath == filePath.toString)

          // Calculate stale data ratio (simplified)
          staleRatio <-
            if (isActive) {
              Async[F].pure(0.0) // Active segments have no stale data
            } else {
              // For inactive segments, estimate stale ratio based on file age
              Async[F].delay {
                val fileAge =
                  System.currentTimeMillis() - segmentName.split("_").lastOption.flatMap(_.toLongOption).getOrElse(0L)
                val maxAge = 24 * 60 * 60 * 1000 // 24 hours
                math.min(fileAge.toDouble / maxAge, 1.0)
              }
            }

          // Count entries in segment (simplified)
          entryCount <- countSegmentEntries(filePath)

        } yield SegmentStats(
          name           = segmentName,
          fileSize       = fileSize,
          isActive       = isActive,
          staleDataRatio = staleRatio,
          entryCount     = entryCount
        )
      }

    } yield segmentStats
  }

  /** Count entries in a segment file */
  private def countSegmentEntries(segmentFile: Path): F[Int] = {
    // Simplified implementation - would need to parse binary format
    for {
      fileSize <- Files[F].size(segmentFile)
      // Assume average entry size of 100 bytes
      entryCount = (fileSize / 100).toInt
    } yield entryCount
  }

  /** Get table statistics (StorageManager manages ONE table) */
  def getStats: F[BitcaskTableStats] = {
    for {
      cacheSnapshot <- cache.get

      // Single pass to calculate entries and data size
      (activeEntries, deletedEntries, totalDataSize) = cacheSnapshot.values.foldLeft((0, 0, 0)) {
        case ((active, deleted, size), entry) =>
          entry match {
            case CacheEntry.Pending(row) =>
              val rowSize = encode(row, config.dataSchema).fold(_ => 0, _.size)
              (active + 1, deleted, size + rowSize)
            case CacheEntry.Persistent(row, _, _) =>
              val rowSize = encode(row, config.dataSchema).fold(_ => 0, _.size)
              (active + 1, deleted, size + rowSize)

            case CacheEntry.Deleted =>
              (active, deleted + 1, size)
          }
      }

      // Group keys by table name (for this single table)
      tableKeys = cacheSnapshot.keys.toList
      tableKeysCount = tableKeys.size
      tableActiveCount = tableKeys.count { key =>
        cacheSnapshot.get(key).exists {
          case CacheEntry.Pending(_) | CacheEntry.Persistent(_, _, _) => true
          case CacheEntry.Deleted                                     => false
        }
      }

      segmentStats <- getSegmentStats

    } yield BitcaskTableStats(
      name               = config.folder.split("/").lastOption.getOrElse("unknown"),
      totalEntries       = tableKeysCount,
      activeEntries      = tableActiveCount,
      deletedEntries     = deletedEntries,
      totalDataSize      = totalDataSize,
      segmentCount       = segmentStats.size,
      activeSegmentCount = segmentStats.count(_.isActive),
      segments           = segmentStats
    )
  }
}

object BitcaskTable {

  private def findLastOffsetInSegment[F[_]: Async: Files](
    targetKey: String,
    path:      String,
    schema:    List[FieldDef]
  ): F[Option[Long]] = {
    readBinary(path, schema)
      .filter(_._2("key").toString == targetKey)
      .map(_._2("offset").asInstanceOf[Long])
      .compile
      .last // Take the most recent offset in this segment
  }

  def initialize[F[_]: Async: Files: Logger](
    name:       String,
    config:     BitcaskTableConfig,
    writeQueue: Channel[F, WriteTask[F]]
  ): F[BitcaskTable[F]] = {

    val tableIndexPath = s"${config.folder}/table.idx"

    for {
      // 1. Read table.idx and build Key -> SegmentName map
      // Stream guarantees that for duplicate keys, the LAST value persists in the Map
      tableMapping <- Files[F].exists(Path(tableIndexPath)).flatMap {
        case false => Async[F].pure(Map.empty[String, String])
        case true =>
          readBinary(tableIndexPath, config.tableSchema)
            .map { (_, row) =>
              row("key").toString -> row("segmentName").toString
            }
            .compile
            .to(Map)
      }

      // 2. Restore CacheEntries based on the mapping
      recoveredCache <- tableMapping
        .toList
        .traverse {
          case (key, "DELETED") =>
            Async[F].pure(Some(key -> CacheEntry.Deleted))

          case (key, segName) =>
            val segIdxPath = s"${config.folder}/$segName.idx"
            val segBinPath = s"${config.folder}/$segName.bin"

            for {
              // Look for the latest key offset in the segment index
              maybeOffset <- findLastOffsetInSegment(key, segIdxPath, config.segmentSchema)

              entry <- maybeOffset match {
                case Some(offset) =>
                  // Read the row from data to warm up the cache
                  readRowAt(segBinPath, config.dataSchema, offset).map {
                    case Some(row) => Some(key -> CacheEntry.Persistent(row, segName, offset))
                    case None      => None
                  }
                case None => Async[F].pure(None)
              }
            } yield entry
        }
        .map(_.flatten.toMap)

      // 3. Create Ref for the cache
      cacheRef <- Ref.of[F, Map[String, CacheEntry]](recoveredCache)

      // 4. Prepare the current active segment
      currentSegName <- Async[F].delay(s"seg_${System.currentTimeMillis()}")

      dataStorage = new BinFileStorage(s"${config.folder}/$currentSegName.bin", config.dataSchema, writeQueue)
      segStorage = new BinFileStorage(s"${config.folder}/$currentSegName.idx", config.segmentSchema, writeQueue)
      tableStorage = new BinFileStorage(tableIndexPath, config.tableSchema, writeQueue)

      dsRef <- Ref.of[F, BinFileStorage[F]](dataStorage)
      ssRef <- Ref.of[F, BinFileStorage[F]](segStorage)

    } yield new BitcaskTable(name, dsRef, ssRef, tableStorage, cacheRef, config, writeQueue)
  }
}
