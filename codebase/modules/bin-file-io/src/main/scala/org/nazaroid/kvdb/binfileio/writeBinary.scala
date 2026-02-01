package org.nazaroid.kvdb.binfileio

import cats.effect.*
import fs2.io.file.{Files, Flags, Path}
import fs2.{Chunk, Stream}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

def writeBinary(
  filePath: String,
  schema:   List[FieldDef],
  data:     List[Row]
): IO[Unit] = {

  def rowToBytes(row: Row): ByteVector = {
    schema.foldLeft(ByteVector.empty) { (acc, field) =>
      val value = row.getOrElse(field.name, throw new Exception(s"field `${field.name}` missing in data"))

      val fieldBytes = field.fType match {
        case FieldType.Int32 =>
          ByteVector.fromInt(value.asInstanceOf[Int])

        case FieldType.Int64 =>
          ByteVector.fromLong(value.asInstanceOf[Long])

        case FieldType.StringUtf8(_) =>
          val bytes = value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
          ByteVector.view(bytes)
      }

      acc ++ fieldBytes
    }
  }

  val byteStream: Stream[IO, Byte] = Stream
    .emits(data)
    .map(rowToBytes)
    .flatMap(bv => Stream.chunk(Chunk.byteVector(bv)))

  byteStream
    .through(
      Files[IO].writeAll(
        Path(filePath),
        Flags(fs2.io.file.Flag.CreateNew, fs2.io.file.Flag.Append)
      )
    )
    .compile
    .drain
}
