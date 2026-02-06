package org.nazaroid.kvdb.binfileio

import cats.effect.*
import fs2.io.file.{Files, Path}
import fs2.{Pull, Stream}

def rowToBytes(row: Row, schema: List[FieldDef]): Chunk[Byte] = {
  val dataBytes = schema.foldLeft(ByteVector.empty) { (acc, field) =>
    val value = row.getOrElse(field.name, throw new Exception(s"Field ${field.name} missing"))
    val fieldBytes = field.fType match {
      case FieldType.Int32 => ByteVector.fromInt(value.asInstanceOf[Int])
      case FieldType.Int64 => ByteVector.fromLong(value.asInstanceOf[Long])
      case FieldType.StringUtf8(_) => ByteVector.view(value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
    }
    acc ++ fieldBytes
  }
  // Добавляем 4 байта (Int32) с длиной данных в начало
  val sizeHeader = ByteVector.fromInt(dataBytes.size.toInt)
  Chunk.byteVector(sizeHeader ++ dataBytes)
}

def decodeSingleRow[F[_]](s: Stream[F, Byte], schema: List[FieldDef]): Pull[F, Nothing, Option[(Row, Stream[F, Byte])]] = {
  // 1. Сначала читаем 4 байта заголовка длины
  s.pull.unconsN(4).flatMap {
    case Some((lenChunk, restAfterLen)) =>
      val rowSize = lenChunk.toByteVector.toInt()

      // 2. Читаем ровно столько байт, сколько указано в заголовке
      restAfterLen.pull.unconsN(rowSize).flatMap {
        case Some((dataChunk, nextStream)) =>
          val row = parseFromChunk(dataChunk, schema)
          Pull.pure(Some((row, nextStream)))
        case None => Pull.pure(None)
      }
    case None => Pull.pure(None)
  }
}

// Вспомогательный метод парсинга уже вычитанного куска
private def parseFromChunk(chunk: Chunk[Byte], schema: List[FieldDef]): Row = {
  val bv = chunk.toByteVector
  schema.foldLeft((Map.empty[String, Any], 0L)) { case ((acc, offset), field) =>
    val size = field.fType match {
      case FieldType.Int32           => 4
      case FieldType.Int64           => 8
      case FieldType.StringUtf8(ref) => acc(ref).toString.toInt
    }
    val slice = bv.slice(offset, offset + size)
    val value = field.fType match {
      case FieldType.Int32         => slice.toInt()
      case FieldType.Int64         => slice.toLong()
      case FieldType.StringUtf8(_) => new String(slice.toArray, StandardCharsets.UTF_8)
    }
    (acc + (field.name -> value), offset + size)
  }._1
}