package org.nazaroid.kvdb.binfileio

import cats.effect.*
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import fs2.{Pull, Stream, Chunk}

def readRowAt[F[_]: Async: Files](
  filePath: String,
  schema:   List[FieldDef],
  offset:   Long
): F[Option[Row]] = {
  val path = Path(filePath)

  // Шаг 1: Читаем заголовок (4 байта)
  Files[F].readRange(path, 4, offset, offset + 4).compile.to(Chunk).flatMap { lenChunk =>
    if (lenChunk.size < 4) Async[F].pure(None)
    else {
      val rowSize = lenChunk.toByteVector.toInt()
      // Шаг 2: Читаем всё тело строки за один раз
      Files[F]
        .readRange(path, rowSize, offset + 4, offset + 4 + rowSize)
        .compile
        .to(Chunk)
        .map { dataChunk =>
          if (dataChunk.size < rowSize) None
          else Some(parseFromChunk(dataChunk, schema))
        }
    }
  }
}
