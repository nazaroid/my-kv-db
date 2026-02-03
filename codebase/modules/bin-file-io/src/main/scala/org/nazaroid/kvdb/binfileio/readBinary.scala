package org.nazaroid.kvdb.binfileio

import cats.effect.*
import fs2.io.file.{Files, Path}
import fs2.{Pull, Stream}

import java.nio.charset.StandardCharsets

def readBinary[F[_]: Async: Files](
  filePath: String,
  schema:   List[FieldDef]
): Stream[F, Row] = {
  def decodeStream(s: Stream[F, Byte]): Pull[F, Row, Unit] = {

    def readFields(
      currentStream:   Stream[F, Byte],
      remainingSchema: List[FieldDef],
      acc:             Row
    ): Pull[F, Row, Unit] = {
      remainingSchema match {
        case Nil =>
          // The record is completely read, we output it and recursively parse the next one
          Pull.output1(acc) >> decodeStream(currentStream)

        case FieldDef(name, fType) :: tail =>
          val bytesToRead = fType match {
            case FieldType.Int32           => Some(4)
            case FieldType.Int64           => Some(8)
            case FieldType.StringUtf8(ref) => acc.get(ref).map(_.toString.toInt)
          }

          bytesToRead match {
            case Some(n) =>
              currentStream.pull.unconsN(n).flatMap {
                case Some((chunk, nextStream)) =>
                  val bv = chunk.toByteVector
                  val value = fType match {
                    case FieldType.Int32         => bv.toInt()
                    case FieldType.Int64         => bv.toLong()
                    case FieldType.StringUtf8(_) => new String(bv.toArray, StandardCharsets.UTF_8)
                  }
                  readFields(nextStream, tail, acc + (name -> value))
                case None => Pull.done // Unexpected end of file within record
              }
            case None =>
              Pull.raiseError[F](new Exception(s"Size field not found for$name"))
          }
      }
    }

    // We check if there is at least 1 byte in the stream before starting to read fields
    s.pull.peek1.flatMap {
      case Some(_) => readFields(s, schema, Map.empty)
      case None    => Pull.done // Natural end of file
    }
  }

  Files[F]
    .readAll(Path(filePath))
    .through(s => decodeStream(s).stream)
}
