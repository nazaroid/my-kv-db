package org.nazaroid.kvdb.bitcask.storage

import cats.effect.*
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.{Files, Flag, Flags, Path}
import org.nazaroid.kvdb.binfileio.*

enum CacheEntry {
  case Pending(row: Row)

  case Persistent(
    row:     Row,
    segment: String,
    offset:  Long)
  case Deleted
}

case class StorageConfig(
  folder:         String,
  maxSegmentSize: Long,
  dataSchema:     List[FieldDef],
  segmentSchema:  List[FieldDef],
  tableSchema:    List[FieldDef])

class BaseStorage[F[_]: Async: Files](
  val filePath: String,
  val schema:   List[FieldDef],
  val queue:    Channel[F, WriteTask[F]]) {

  def append(id: String, row: Row): F[Long] =
    for {
      promise <- Deferred[F, Long]
      _       <- queue.send(WriteTask(id, filePath, schema, row, Some(promise)))
      offset  <- promise.get
    } yield offset
}

sealed class StorageManager[F[_]: Async: Files](
  val currentData:       Ref[F, BaseStorage[F]],
  val currentSegmentIdx: Ref[F, BaseStorage[F]],
  val tableStorage:      BaseStorage[F],
  val cache:             Ref[F, Map[String, CacheEntry]],
  val config:            StorageConfig,
  val writeQueue:        Channel[F, WriteTask[F]]) {

  /** WRITE: Data -> Segment -> Table -> Cache */
  def write(key: String, value: String): F[Unit] = {
    val dataRow = Map(
      "recordSize" -> value.getBytes("UTF-8").length,
      "value"      -> value
    )
    for {
      _ <- cache.update(_ + (key -> CacheEntry.Pending(dataRow)))

      ds   <- currentData.get
      size <- Files[F].size(Path(ds.filePath)).handleError(_ => 0L)

      // Rotate if the current .bin file exceeds max size
      activeDS <- if (size > config.maxSegmentSize) rotate() else Async[F].pure(ds)
      activeSS <- currentSegmentIdx.get

      // Cascading write operations
      offset <- activeDS.append(key, dataRow)

      segRow = Map("keySize" -> key.length, "key" -> key, "offset" -> offset)
      _ <- activeSS.append(key, segRow)

      segName = Path(activeDS.filePath).fileName.toString.replace(".bin", "")
      tableRow = Map(
        "keySize"         -> key.length,
        "key"             -> key,
        "segmentNameSize" -> segName.length,
        "segmentName"     -> segName
      )
      _ <- tableStorage.append(key, tableRow)

      _ <- cache.update(_ + (key -> CacheEntry.Persistent(dataRow, segName, offset)))
    } yield ()
  }

  /** READ: Always from cache (O(1)) */
  def read(key: String): F[Option[String]] = {
    cache
      .get
      .map(_.get(key).flatMap {
        case CacheEntry.Pending(row)          => row.get("value").map(_.toString)
        case CacheEntry.Persistent(row, _, _) => row.get("value").map(_.toString)
        case CacheEntry.Deleted               => None
      })
  }

  /** DELETE: Write a Tombstone record */
  def delete(key: String): F[Unit] = {
    for {
      _ <- cache.update(_ + (key -> CacheEntry.Deleted))
      tRow = Map("keySize" -> key.length, "key" -> key, "segmentNameSize" -> 7, "segmentName" -> "DELETED")
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
              bytes <- Async[F].delay(encode(row, config.dataSchema))
              _     <- Stream.chunk(bytes).through(Files[F].writeAll(cDataPath, Flags(Flag.Append))).compile.drain
              off   <- Files[F].size(cDataPath).map(_ - bytes.size)

              iRow = Map("keySize" -> key.length, "key" -> key, "offset" -> off)
              iBytes <- Async[F].delay(encode(iRow, config.segmentSchema))
              _      <- Stream.chunk(iBytes).through(Files[F].writeAll(cIdxPath, Flags(Flag.Append))).compile.drain

              tRow = Map(
                "keySize"         -> key.length,
                "key"             -> key,
                "segmentNameSize" -> compName.length,
                "segmentName"     -> compName
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

  private def rotate(): F[BaseStorage[F]] = {
    val name = s"seg_${System.currentTimeMillis()}"
    val nDS = new BaseStorage(s"${config.folder}/$name.bin", config.dataSchema, writeQueue)
    val nSS = new BaseStorage(s"${config.folder}/$name.idx", config.segmentSchema, writeQueue)
    currentData.set(nDS) *> currentSegmentIdx.set(nSS) *> Async[F].pure(nDS)
  }
}

object StorageManager {

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

  def initialize[F[_]: Async: Files](
    config:     StorageConfig,
    writeQueue: Channel[F, WriteTask[F]]
  ): F[StorageManager[F]] = {

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

      dataStorage = new BaseStorage(s"${config.folder}/$currentSegName.bin", config.dataSchema, writeQueue)
      segStorage = new BaseStorage(s"${config.folder}/$currentSegName.idx", config.segmentSchema, writeQueue)
      tableStorage = new BaseStorage(tableIndexPath, config.tableSchema, writeQueue)

      dsRef <- Ref.of[F, BaseStorage[F]](dataStorage)
      ssRef <- Ref.of[F, BaseStorage[F]](segStorage)

    } yield new StorageManager(dsRef, ssRef, tableStorage, cacheRef, config, writeQueue)
  }
}
