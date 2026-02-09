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

// --- 2. Базовое хранилище (Низкоуровневый Writer) ---
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

// --- 3. Stratum Storage Manager ---
sealed class StorageManager[F[_]: Async: Files](
  val currentData:       Ref[F, BaseStorage[F]],
  val currentSegmentIdx: Ref[F, BaseStorage[F]],
  val tableStorage:      BaseStorage[F],
  val cache:             Ref[F, Map[String, CacheEntry]],
  val config:            StorageConfig,
  val writeQueue:        Channel[F, WriteTask[F]]) {

  /** ЗАПИСЬ: Data -> Segment -> Table -> Cache */
  def write(key: String, value: String): F[Unit] = {
    val dataRow = Map(
      "recordSize" -> value.getBytes("UTF-8").length,
      "value"      -> value
    )
    for {
      _ <- cache.update(_ + (key -> CacheEntry.Pending(dataRow)))

      ds   <- currentData.get
      size <- Files[F].size(Path(ds.filePath)).handleError(_ => 0L)

      // Ротация если текущий .bin переполнен
      activeDS <- if (size > config.maxSegmentSize) rotate() else Async[F].pure(ds)
      activeSS <- currentSegmentIdx.get

      // Каскадная запись
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

  /** ЧТЕНИЕ: Всегда из кэша (O(1)) */
  def read(key: String): F[Option[String]] = {
    cache
      .get
      .map(_.get(key).flatMap {
        case CacheEntry.Pending(row)          => row.get("value").map(_.toString)
        case CacheEntry.Persistent(row, _, _) => row.get("value").map(_.toString)
        case CacheEntry.Deleted               => None
      })
  }

  /** УДАЛЕНИЕ: Запись Tombstone */
  def delete(key: String): F[Unit] = {
    for {
      _ <- cache.update(_ + (key -> CacheEntry.Deleted))
      tRow = Map("keySize" -> key.length, "key" -> key, "segmentNameSize" -> 7, "segmentName" -> "DELETED")
      _ <- tableStorage.append(key, tRow)
    } yield ()
  }

  /** COMPACTION: Уплотнение всех сегментов в один новый */
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
              bytes <- Async[F].delay(rowToBytes(row, config.dataSchema))
              _     <- Stream.chunk(bytes).through(Files[F].writeAll(cDataPath, Flags(Flag.Append))).compile.drain
              off   <- Files[F].size(cDataPath).map(_ - bytes.size)

              iRow = Map("keySize" -> key.length, "key" -> key, "offset" -> off)
              iBytes <- Async[F].delay(rowToBytes(iRow, config.segmentSchema))
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

  /** CLEANUP: Физическое удаление "осиротевших" файлов */
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
      .last // Берем самое свежее смещение в этом сегменте
  }

  def initialize[F[_]: Async: Files](
    config:     StorageConfig,
    writeQueue: Channel[F, WriteTask[F]]
  ): F[StorageManager[F]] = {

    val tableIndexPath = s"${config.folder}/table.idx"

    for {
      // 1. Читаем table.idx и строим карту Key -> SegmentName
      // Stream гарантирует, что при дубликатах ключей в Map попадет ПОСЛЕДНЕЕ значение
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

      // 2. На основе маппинга восстанавливаем CacheEntry
      recoveredCache <- tableMapping
        .toList
        .traverse {
          case (key, "DELETED") =>
            Async[F].pure(Some(key -> CacheEntry.Deleted))

          case (key, segName) =>
            val segIdxPath = s"${config.folder}/$segName.idx"
            val segBinPath = s"${config.folder}/$segName.bin"

            for {
              // Ищем последний offset ключа в индексе сегмента
              maybeOffset <- findLastOffsetInSegment(key, segIdxPath, config.segmentSchema)

              entry <- maybeOffset match {
                case Some(offset) =>
                  // Читаем саму строку из данных для прогрева кэша
                  readRowAt(segBinPath, config.dataSchema, offset).map {
                    case Some(row) => Some(key -> CacheEntry.Persistent(row, segName, offset))
                    case None      => None
                  }
                case None => Async[F].pure(None)
              }
            } yield entry
        }
        .map(_.flatten.toMap)

      // 3. Создаем Ref для кэша
      cacheRef <- Ref.of[F, Map[String, CacheEntry]](recoveredCache)

      // 4. Подготавливаем текущий активный сегмент
      currentSegName <- Async[F].delay(s"seg_${System.currentTimeMillis()}")

      dataStorage = new BaseStorage(s"${config.folder}/$currentSegName.bin", config.dataSchema, writeQueue)
      segStorage = new BaseStorage(s"${config.folder}/$currentSegName.idx", config.segmentSchema, writeQueue)
      tableStorage = new BaseStorage(tableIndexPath, config.tableSchema, writeQueue)

      dsRef <- Ref.of[F, BaseStorage[F]](dataStorage)
      ssRef <- Ref.of[F, BaseStorage[F]](segStorage)

    } yield new StorageManager(dsRef, ssRef, tableStorage, cacheRef, config, writeQueue)
  }
}
