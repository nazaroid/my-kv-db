package org.nazaroid.kvdb.binfileio

import cats.effect.*
import fs2.io.file.{Files, Path}
import fs2.{Pull, Stream}

import java.nio.charset.StandardCharsets

def readBinary[F[_]: Async: Files](
                                              filePath: String,
                                              schema: List[FieldDef]
                                            ): Stream[F, (Long, Row)] = {

  def loop(s: Stream[F, Byte], currentOffset: Long): Pull[F, (Long, Row), Unit] = {
    // 1. Пытаемся считать заголовок длины (4 байта)
    s.pull.unconsN(4).flatMap {
      case Some((lenChunk, restAfterLen)) =>
        val rowSize = lenChunk.toByteVector.toInt()

        // 2. Считываем само тело записи
        restAfterLen.pull.unconsN(rowSize).flatMap {
          case Some((dataChunk, nextStream)) =>
            val row = parseFromChunk(dataChunk, schema)
            // Выдаем текущий offset (начало 4-байтового заголовка) и данные
            Pull.output1((currentOffset, row)) >>
              loop(nextStream, currentOffset + 4 + rowSize)
          case None =>
            Pull.done
        }
      case None =>
        Pull.done
    }
  }

  Files[F]
    .readAll(Path(filePath))
    .through(s => loop(s, 0L).stream)
}