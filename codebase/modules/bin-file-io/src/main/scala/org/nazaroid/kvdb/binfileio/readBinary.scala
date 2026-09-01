package org.nazaroid.kvdb.binfileio

import cats.effect.Async
import fs2.io.file.{Files, Path}
import fs2.{Pull, Stream}
import org.typelevel.log4cats.Logger

def readBinary[F[_]: Async: Files: Logger](
  filePath: String,
  schema:   List[FieldDef]
): Stream[F, (Long, Row)] = {

  def loop(s: Stream[F, Byte], currentOffset: Long): Pull[F, (Long, Row), Unit] = {
    s.pull.unconsN(4, allowFewer = false).flatMap {
      case Some((lenChunk, restAfterLen)) =>
        val rowSize = lenChunk.toByteVector.toInt()
        val nextOffsetAfterHeader = currentOffset + 4

        restAfterLen.pull.unconsN(rowSize, allowFewer = false).flatMap {
          case Some((dataChunk, nextStream)) =>
            decode(dataChunk, schema) match {
              case Right(row) =>
                Pull.output1((currentOffset, row)) >>
                  loop(nextStream, nextOffsetAfterHeader + rowSize)

              case Left(error) =>
                Pull.eval(Logger[F].error(s"Error decoding at offset $currentOffset: $error")) >>
                  loop(nextStream, nextOffsetAfterHeader + rowSize)
            }

          case None =>
            Pull.eval(Logger[F].warn(s"Unexpected EOF while reading row body of size $rowSize")) >> Pull.done
        }

      case None =>
        Pull.done
    }
  }

  Files[F]
    .readAll(Path(filePath))
    .through(s => loop(s, 0L).stream)
}
