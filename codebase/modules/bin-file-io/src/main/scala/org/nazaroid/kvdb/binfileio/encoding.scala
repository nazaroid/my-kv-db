package org.nazaroid.kvdb.binfileio

import fs2.Chunk
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

def encode(row: Row, schema: List[FieldDef]): Chunk[Byte] = {
  // Encode data
  val dataBytes = schema.foldLeft(ByteVector.empty) { (acc, field) =>
    if (field.fType == FieldType.CRC32) {
      acc
    } else {
      val value = row.getOrElse(field.name, throw new Exception(s"Field ${field.name} missing"))
      val fieldBytes = field.fType match {
        case FieldType.Int32         => ByteVector.fromInt(value.asInstanceOf[Int])
        case FieldType.Int64         => ByteVector.fromLong(value.asInstanceOf[Long])
        case FieldType.StringUtf8(_) => ByteVector.view(value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
        case FieldType.Timestamp     => ByteVector.fromLong(value.asInstanceOf[Long])
        case FieldType.RecordStatus  => ByteVector.fromInt(value.asInstanceOf[Int])
        case t: FieldType            => throw new UnsupportedOperationException(s"Unsupported field type: $t")
      }
      acc ++ fieldBytes
    }
  }

  // Add CRC only if it exists in schema
  val finalBytes = if (schema.exists(_.fType == FieldType.CRC32)) {
    val crcValue = CRC32Calculator.calculate(dataBytes.toArray)
    dataBytes ++ ByteVector.fromLong(crcValue)
  } else {
    dataBytes
  }

  val sizeHeader = ByteVector.fromInt(finalBytes.size.toInt)
  Chunk.byteVector(sizeHeader ++ finalBytes)
}

def decode(chunk: Chunk[Byte], schema: List[FieldDef]): Either[String, Row] = {
  val bv = chunk.toByteVector

  // Verify CRC only if it exists in schema
  if (schema.exists(_.fType == FieldType.CRC32) && bv.size >= 8) {
    val dataWithoutCRC = bv.dropRight(8) // Remove CRC at the end
    val expectedCRC = bv.takeRight(8).toLong()
    val actualCRC = CRC32Calculator.calculate(dataWithoutCRC.toArray)

    if (actualCRC != expectedCRC) {
      return Left(s"CRC mismatch: expected $expectedCRC, actual $actualCRC")
    }

    parseFields(bv, schema)
  } else if (schema.exists(_.fType == FieldType.CRC32)) {
    Left("Data too small for CRC validation")
  } else {
    parseFields(bv, schema)
  }
}

private def parseFields(bv: ByteVector, schema: List[FieldDef]): Either[String, Row] = {
  try {
    val result = schema
      .foldLeft((Map.empty[String, Any], 0L)) { case ((acc, offset), field) =>
        if (field.fType == FieldType.CRC32) {
          (acc, offset) // CRC32 is not parsed, always 0
        } else {
          val size = field.fType match {
            case FieldType.Int32           => 4
            case FieldType.Int64           => 8
            case FieldType.StringUtf8(ref) => acc(ref).toString.toInt
            case FieldType.Timestamp       => 8
            case FieldType.RecordStatus    => 4
            case t: FieldType              => throw new UnsupportedOperationException(s"Unsupported field type: $t")
          }

          if (offset + size > bv.size) {
            throw new Exception(s"Field ${field.name} exceeds data size")
          }

          val slice = bv.slice(offset, offset + size)
          val value = field.fType match {
            case FieldType.Int32         => slice.toInt()
            case FieldType.Int64         => slice.toLong()
            case FieldType.StringUtf8(_) => new String(slice.toArray, StandardCharsets.UTF_8)
            case FieldType.Timestamp     => slice.toLong()
            case FieldType.RecordStatus  => slice.toInt()
            case t: FieldType            => throw new UnsupportedOperationException(s"Unsupported field type: $t")
          }
          (acc + (field.name -> value), offset + size)
        }
      }
      ._1

    Right(result)
  } catch {
    case e: Exception => Left(s"Decoding error: ${e.getMessage}")
  }
}
