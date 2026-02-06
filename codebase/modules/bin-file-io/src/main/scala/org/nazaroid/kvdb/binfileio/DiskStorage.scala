package org.nazaroid.kvdb.binfileio

import cats.effect.{Async, Deferred, Ref, Async as Files, *}
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import fs2.{Pull, Stream}
import org.nazaroid.kvdb.binfileio.{FieldDef, DiskStorageValue, WriteTask}

// Состояние индекса для одного файла
// Any - это идентификатор записи (например, UUID или String)
enum DiskStorageValue {
  case Pending(row: Row) // Данные в памяти, еще не на диске
  case OnDisk(offset: Long) // Данные на диске по адресу
}

type Index[F[_]] = Ref[F, Map[String, DiskStorageValue]]

class DiskStorage[F[_]: Async: Files](
  index:      Index[F],
  writeQueue: Channel[F, WriteTask],
  filePath:   String,
  schema:     List[FieldDef],
  idField:    String) {

  // ЧТЕНИЕ: Сначала смотрим в индекс, потом на диск
  def read(key: String): F[Option[Row]] = {
    index.get.flatMap { m =>
      m.get(key) match {
        case Some(DiskStorageValue.Pending(row)) =>
          Async[F].pure(Some(row)) // Мгновенно из кеша

        case Some(DiskStorageValue.OnDisk(offset)) =>
          readRowAt(filePath, schema, offset) // Из файла по смещению

        case None =>
          Async[F].pure(None)
      }
    }
  }

  // ЗАПИСЬ: Кладем в кеш и отправляем в writeBinary
  def write(
    key: String,
    row: Row
  ): F[Unit] = {
    for {
      // 1. Ставим в индекс как "Pending" (для Read-Your-Writes)
      _ <- index.update(_ + (key -> DiskStorageValue.Pending(row)))

      // 2. Создаем обещание смещения
      promise <- Deferred[F, Long]

      // 3. Отправляем в очередь (вам нужно добавить callback в WriteTask, как мы делали ранее)
      task = WriteTask(filePath, schema, row, Some(promise))
      _ <- writeQueue.send(task)

      // 4. Запускаем фоновое обновление индекса
      _ <- Async[F].start {
        promise.get.flatMap { offset =>
          index.update(_ + (key -> DiskStorageValue.OnDisk(offset)))
        }
      }
    } yield ()
  }

  def recoverIndex(): F[Unit] = {
    def readIndex(
      filePath: String,
      schema:   List[FieldDef],
      idField:  String
    ): F[Map[String, DiskStorageValue]] = {
      val path = Path(filePath)
      Files[F].exists(Path(filePath)).flatMap {
        case false => Async[F].pure(Map.empty)
        case true =>
          readBinary(filePath, schema)
            .map { (offset, row) =>
              val id = row(idField).toString
              id -> StorageValue.OnDisk(offset)
            }
            .compile
            .to(Map)
      }
    }

    readIndex(filePath, schema, idField) >>= index.update
  }
}
