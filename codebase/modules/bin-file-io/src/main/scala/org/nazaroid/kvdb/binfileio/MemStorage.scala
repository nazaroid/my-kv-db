package org.nazaroid.kvdb.binfileio

import cats.effect.*
import cats.syntax.all.*
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}

enum MemStorageValue {
  // Данные в очереди на запись
  case Pending(row: Row)
  // Данные записаны, но мы продолжаем держать Row в памяти для скорости
  case Persistent(row: Row, offset: Long)
}

class MemStorage[F[_]: Async: Files](
  val index:  Ref[F, Map[String, MemStorageValue]],
  writeQueue: Channel[F, WriteTask[F]],
  schema:     List[FieldDef],
  filePath:   String,
  idField:    String) {

  // ЧТЕНИЕ: Максимально быстрое, так как Row всегда в памяти
  def read(id: String): F[Option[Row]] = {
    index.get.map { m =>
      m.get(id).map {
        case MemStorageValue.Pending(row)       => row
        case MemStorageValue.Persistent(row, _) => row
      }
    }
  }

  // ЗАПИСЬ: Кладем в кеш и ждем подтверждения смещения
  def write(id: String, row: Row): F[Unit] = {
    for {
      // 1. Сразу фиксируем в кеше как Pending
      _ <- index.update(_ + (id -> MemStorageValue.Pending(row)))

      promise <- Deferred[F, Long]
      _       <- writeQueue.send(WriteTask(id, filePath, schema, row, Some(promise)))

      // 2. Фоновое обновление: когда запишется на диск, обновляем статус,
      // но сохраняем Row внутри Persistent
      _ <- Async[F].start {
        promise.get.flatMap { off =>
          index.update { m =>
            m.get(id) match {
              case Some(MemStorageValue.Pending(r)) =>
                m + (id -> MemStorageValue.Persistent(r, off))
              case _ => m // На случай, если данные уже обновились новой записью
            }
          }
        }
      }
    } yield ()
  }

  def recoverIndex(): F[Unit] = {
    def readIndex(
      filePath: String,
      schema:   List[FieldDef],
      idField:  String
    ): F[Map[String, MemStorageValue]] = {
      val path = Path(filePath)

      Files[F].exists(Path(filePath)).flatMap {
        case false => Async[F].pure(Map.empty)
        case true =>
          readBinary(filePath, schema)
            .map { (offset, row) =>
              val id = row(idField).toString
              id -> MemStorageValue.Persistent(row, offset)
            }
            .compile
            .to(Map)
      }
    }

    readIndex(filePath, schema, idField) >>= index.set
  }
}
