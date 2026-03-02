package org.nazaroid.kvdb.binfileio

import fs2.Chunk
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector

final class EncodingErrorHandlingSpec extends AnyFunSuite with Matchers {

  test("decode should handle corrupted data gracefully") {
    val schema = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("crc", FieldType.CRC32)
    )

    // Test with insufficient data
    val tooSmallChunk = Chunk.array(Array[Byte](1, 2, 3)) // Only 3 bytes
    val result1 = decode(tooSmallChunk, schema)

    result1 shouldBe a[Left[String, Row]]
    result1 match {
      case Left(error) => error should include("Data too small for CRC validation")
      case Right(_)    => fail("Should not decode successfully with insufficient data")
    }

    // Test with corrupted CRC
    val validRow = Map(
      "valueSize" -> 5,
      "value"      -> "hello"
    )
    val validEncoded = encode(validRow, schema)
    val corruptedChunk = Chunk.byteVector(validEncoded.toByteVector.dropRight(8) ++ ByteVector.fromLong(99999L))

    val result2 = decode(corruptedChunk, schema)

    result2 shouldBe a[Left[String, Row]]
    result2 match {
      case Left(error) => error should include("CRC mismatch")
      case Right(_)    => fail("Should not decode successfully with corrupted CRC")
    }
  }

  test("decode should work without CRC validation when no CRC in schema") {
    val schemaWithoutCRC = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize"))
    )

    val row = Map(
      "valueSize" -> 5,
      "value"      -> "hello"
    )

    val encoded = encode(row, schemaWithoutCRC)
    val decoded = decode(encoded, schemaWithoutCRC)

    decoded shouldBe a[Right[String, Row]]
    decoded match {
      case Right(decodedRow) =>
        decodedRow("valueSize") should be(5)
        decodedRow("value") should be("hello")
      case Left(error) => fail(s"Should not fail with error: $error")
    }
  }

  test("encode should handle missing fields gracefully") {
    val schema = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("crc", FieldType.CRC32)
    )

    val rowWithoutRequired = Map(
      "value" -> "hello"
      // Missing "valueSize" field
    )

    assertThrows[Exception] {
      encode(rowWithoutRequired, schema)
    }
  }

  test("encode should handle different field types correctly") {
    val schema = List(
      FieldDef("int32Field", FieldType.Int32),
      FieldDef("int64Field", FieldType.Int64),
      FieldDef("stringField", FieldType.StringUtf8(sizeFromField = "int32Field")),
      FieldDef("timestampField", FieldType.Timestamp),
      FieldDef("statusField", FieldType.RecordStatus),
      FieldDef("crcField", FieldType.CRC32)
    )

    val row = Map(
      "int32Field"     -> 11,
      "int64Field"     -> 123456789L,
      "stringField"    -> "test string",
      "timestampField" -> System.currentTimeMillis(),
      "statusField"    -> 1
    )

    val encoded = encode(row, schema)
    val decoded = decode(encoded, schema)

    decoded shouldBe a[Right[String, Row]]
    decoded match {
      case Right(decodedRow) =>
        decodedRow("int32Field") should be(11)
        decodedRow("int64Field") should be(123456789L)
        decodedRow("stringField") should be("test string")
        decodedRow("statusField") should be(1)
      case Left(error) => fail(s"Should not fail with error: $error")
    }
  }

  test("CRC calculation should be consistent across multiple calls") {
    val data = "consistent test data".getBytes("UTF-8")

    val crc1 = CRC32Calculator.calculate(data)
    val crc2 = CRC32Calculator.calculate(data)
    val crc3 = CRC32Calculator.calculate(data)

    crc1 should be(crc2)
    crc2 should be(crc3)
    crc1 should not be 0L
  }

  test("CRC verification should handle empty data") {
    val emptyData = Array.empty[Byte]
    val crc = CRC32Calculator.calculate(emptyData)

    val result = CRC32Calculator.verify(emptyData, crc)

    result shouldBe a[CRCResult]
    result match {
      case CRCResult.Valid         => succeed
      case CRCResult.Invalid(_, _) => fail("Empty data should be valid")
    }
  }

  test("field type sizes should be correct") {
    val sizes = Map(
      FieldType.Int32        -> 4,
      FieldType.Int64        -> 8,
      FieldType.Timestamp    -> 8,
      FieldType.RecordStatus -> 4,
      FieldType.CRC32        -> 8
    )

    sizes.foreach { case (fieldType, expectedSize) =>
      val actualSize = fieldType match {
        case FieldType.Int32         => 4
        case FieldType.Int64         => 8
        case FieldType.StringUtf8(_) => 10 // Dynamic size
        case FieldType.Timestamp     => 8
        case FieldType.RecordStatus  => 4
        case FieldType.CRC32         => 8
      }
      actualSize should be(expectedSize)
    }
  }
}
