package org.nazaroid.kvdb.binfileio

import cats.syntax.all.*
import cats.effect.*
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.WriteTask
import fs2.concurrent.Channel

// Состояние индекса для одного файла
// Any - это идентификатор записи (например, UUID или String)
enum DataStorageValue {
  case Pending(row: Row) // Данные в памяти, еще не на диске
  case OnDisk(offset: Long) // Данные на диске по адресу
}


class DataStorage[F[_]: Async: Files](
  writeQueue: Channel[F, WriteTask[F]],
  filePath:   String,
  schema:     List[FieldDef],
  ixStorage: MemStorage[F]) {

  // ЧТЕНИЕ: Сначала смотрим в индекс, потом на диск
  def read(offset: Long): F[Option[Row]] = {
    readRowAt(filePath, schema, offset)
  }

  def write(
    key: String,
    row: Row
  ): F[Unit] = {
    for {
      promise <- Deferred[F, Long]
      // 3. Отправляем в очередь (вам нужно добавить callback в WriteTask, как мы делали ранее)
      task = WriteTask(key, filePath, schema, row, Some(promise))
      _ <- writeQueue.send(task)

      // 4. Запускаем фоновое обновление индекса
      _ <- Async[F].start {
        promise.get.flatMap { offset =>
          ixStorage.write(key, )
        }
      }
    } yield ()
  }

  def recoverIndex(): F[Unit] = {
    def readIndex(
      filePath: String,
      schema:   List[FieldDef],
      idField:  String
    ): F[Map[String, DataStorageeValue]] = {
      val path = Path(filePath)
      Files[F].exists(Path(filePath)).flatMap {
        case false => Async[F].pure(Map.empty)
        case true =>
          readBinary(filePath, schema)
            .map { (offset, row) =>
              val id = row(idField).toString
              id -> DataStorageeValue.OnDisk(offset)
            }
            .compile
            .to(Map)
      }
    }

    readIndex(filePath, schema, idField) >>= index.set
  }
}
