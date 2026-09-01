package org.nazaroid.kvdb.binfileio

import fs2.Chunk
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import scala.util.Try

def utf8ByteLength(s: String): Int = s.getBytes(StandardCharsets.UTF_8).length

def encode(row: Row, schema: List[FieldDef]): Either[String, Chunk[Byte]] =
  schema
    .foldLeft[Either[String, ByteVector]](Right(ByteVector.empty)) { (accE, field) =>
      accE.flatMap { acc =>
        if (field.fType == FieldType.CRC32) {
          Right(acc)
        } else {
          row.get(field.name) match {
            case None => Left(s"Field ${field.name} missing")
            case Some(value) =>
              encodeFieldValue(row, field, value).map(acc ++ _)
          }
        }
      }
    }
    .map { dataBytes =>
      val finalBytes = if (schema.exists(_.fType == FieldType.CRC32)) {
        val crcValue = CRC32Calculator.calculate(dataBytes.toArray)
        dataBytes ++ ByteVector.fromLong(crcValue)
      } else {
        dataBytes
      }
      Chunk.byteVector(finalBytes)
    }

private def encodeFieldValue(row: Row, field: FieldDef, value: Any): Either[String, ByteVector] =
  field.fType match {
    case FieldType.Int32 => Right(ByteVector.fromInt(value.asInstanceOf[Int]))
    case FieldType.Int64 => Right(ByteVector.fromLong(value.asInstanceOf[Long]))
    case FieldType.StringUtf8(ref) =>
      val str = value.asInstanceOf[String]
      val bytes = str.getBytes(StandardCharsets.UTF_8)
      val declaredSize = row.get(ref).map(_.toString.toInt).getOrElse(bytes.length)
      /* Encode always writes the full UTF-8 payload (bytes.length), while decode reads
         exactly declaredSize bytes from the size prefix field (e.g. keySize). If callers
         set the prefix to the wrong value (e.g. String.length instead of UTF-8 byte
         length), the on-disk record becomes misaligned and later fields decode incorrectly.
       */
      if (declaredSize != bytes.length) {
        Left(
          s"Field '${field.name}': size field '$ref' declares $declaredSize bytes but UTF-8 encoding is ${bytes.length} bytes"
        )
      } else {
        Right(ByteVector.view(bytes))
      }
    case FieldType.Timestamp    => Right(ByteVector.fromLong(value.asInstanceOf[Long]))
    case FieldType.RecordStatus => Right(ByteVector.fromInt(value.asInstanceOf[Int]))
    case FieldType.CRC32        => Left(s"Field ${field.name}: CRC32 is not encoded as a row value")
  }

def decode(chunk: Chunk[Byte], schema: List[FieldDef]): Either[String, Row] = {
  val bv = chunk.toByteVector

  // Verify CRC only if it exists in schema
  if (schema.exists(_.fType == FieldType.CRC32) && bv.size >= 8) {
    val dataWithoutCRC = bv.dropRight(8) // Remove CRC at the end
    val expectedCRC = bv.takeRight(8).toLong()
    val actualCRC = CRC32Calculator.calculate(dataWithoutCRC.toArray)

    if (actualCRC != expectedCRC) {
      Left(s"CRC mismatch: expected $expectedCRC, actual $actualCRC")
    } else {
      parseFields(bv, schema)
    }
  } else if (schema.exists(_.fType == FieldType.CRC32)) {
    Left("Data too small for CRC validation")
  } else {
    parseFields(bv, schema)
  }
}

private def parseFields(bv: ByteVector, schema: List[FieldDef]): Either[String, Row] =
  schema
    .foldLeft[Either[String, (Map[String, Any], Long)]](Right((Map.empty, 0L))) { (accE, field) =>
      accE.flatMap { case (acc, offset) =>
        if (field.fType == FieldType.CRC32) {
          Right((acc, offset))
        } else {
          for {
            size <- fieldSize(field, acc)
            _ <-
              if (offset + size > bv.size) Left(s"Field ${field.name} exceeds data size")
              else Right(())
            value = decodeFieldValue(field, bv.slice(offset, offset + size))
          } yield (acc + (field.name -> value), offset + size)
        }
      }
    }
    .map(_._1)

private def fieldSize(field: FieldDef, acc: Map[String, Any]): Either[String, Int] =
  field.fType match {
    case FieldType.Int32        => Right(4)
    case FieldType.Int64        => Right(8)
    case FieldType.StringUtf8(ref) =>
      acc.get(ref) match {
        case None =>
          Left(s"Size field '$ref' missing for field '${field.name}'")
        case Some(v) =>
          Try(v.toString.toInt).toEither.left.map(_ => s"Invalid size value for field '$ref'")
      }
    case FieldType.Timestamp    => Right(8)
    case FieldType.RecordStatus => Right(4)
    case FieldType.CRC32        => Left(s"Field ${field.name}: CRC32 is not parsed as a row value")
  }

private def decodeFieldValue(field: FieldDef, slice: ByteVector): Any =
  field.fType match {
    case FieldType.Int32         => slice.toInt()
    case FieldType.Int64         => slice.toLong()
    case FieldType.StringUtf8(_) => new String(slice.toArray, StandardCharsets.UTF_8)
    case FieldType.Timestamp     => slice.toLong()
    case FieldType.RecordStatus  => slice.toInt()
    case FieldType.CRC32         => slice.toLong()
  }
