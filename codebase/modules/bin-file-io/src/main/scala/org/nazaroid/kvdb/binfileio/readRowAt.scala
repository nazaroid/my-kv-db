package org.nazaroid.kvdb.binfileio

import cats.effect.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.{Files, Path}

def readRowAt[F[_]: Async: Files](
  filePath: String,
  schema:   List[FieldDef],
  offset:   Long
): F[Option[Row]] = {
  val path = Path(filePath)

  // Step 1: Read the header (4 bytes)
  Files[F].readRange(path, 4, offset, offset + 4).compile.to(Chunk).flatMap { lenChunk =>
    if (lenChunk.size < 4) Async[F].pure(None)
    else {
      val rowSize = lenChunk.toByteVector.toInt()
      // Step 2: Read the entire row body in one go
      Files[F]
        .readRange(path, rowSize, offset + 4, offset + 4 + rowSize)
        .compile
        .to(Chunk)
        .map { dataChunk =>
          if (dataChunk.size < rowSize) None
          else Some(decode(dataChunk, schema))
        }
    }
  }
}
