package org.nazaroid.kvdb.binfileio

import cats.effect.Async
import fs2.io.file.{Files, Path}
import fs2.{Pull, Stream}

def readBinary[F[_]: Async: Files](
  filePath: String,
  schema:   List[FieldDef]
): Stream[F, (Long, Row)] = {

  def loop(s: Stream[F, Byte], currentOffset: Long): Pull[F, (Long, Row), Unit] = {
    // 1. Attempt to read the length header (4 bytes)
    s.pull.unconsN(4).flatMap {
      case Some((lenChunk, restAfterLen)) =>
        val rowSize = lenChunk.toByteVector.toInt()

        // 2. Read the actual record body
        restAfterLen.pull.unconsN(rowSize).flatMap {
          case Some((dataChunk, nextStream)) =>
            decode(dataChunk, schema) match {
              case Right(row) =>
                // Emit current offset (start of the 4-byte header) and the decoded data
                Pull.output1((currentOffset, row)) >>
                  loop(nextStream, currentOffset + 4 + rowSize)
              case Left(error) =>
                // TODO: Log error and continue
                println(error)
                Pull.done // Skip corrupted record
            }
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
