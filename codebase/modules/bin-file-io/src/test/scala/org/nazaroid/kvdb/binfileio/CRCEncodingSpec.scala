package org.nazaroid.kvdb.binfileio

import fs2.Chunk
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector

final class CRCEncodingSpec extends AnyFunSuite with Matchers {

  test("encode should add CRC only when FieldType.CRC32 is in schema") {
    val schemaWithCRC = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("crc", FieldType.CRC32)
    )

    val schemaWithoutCRC = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize"))
    )

    val row = Map(
      "valueSize" -> 5,
      "value"      -> "hello"
    )

    val encodedWithCRC = encode(row, schemaWithCRC)
    val encodedWithoutCRC = encode(row, schemaWithoutCRC)

    // With CRC: should be larger (data + 8 bytes CRC)
    encodedWithCRC.size should be > encodedWithoutCRC.size
    encodedWithCRC.size should be(encodedWithoutCRC.size + 8)
  }

  test("decode should verify CRC when FieldType.CRC32 is in schema") {
    val schema = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("crc", FieldType.CRC32)
    )

    val row = Map(
      "valueSize" -> 5,
      "value"      -> "hello"
    )

    val encoded = encode(row, schema)
    val decoded = decode(encoded, schema)

    decoded shouldBe a[Right[String, Row]]
    decoded match {
      case Right(decodedRow) =>
        decodedRow("valueSize") should be(5)
        decodedRow("value") should be("hello")
      case Left(error) => fail(s"Should not fail with error: $error")
    }
  }

  test("decode should return Left when CRC mismatch") {
    val schema = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize")),
      FieldDef("crc", FieldType.CRC32)
    )

    val row = Map(
      "valueSize" -> 5,
      "value"      -> "hello"
    )

    val encoded = encode(row, schema)
    // Corrupt the CRC
    val corrupted = encoded.toByteVector.dropRight(8) ++ ByteVector.fromLong(12345L)

    val decoded = decode(Chunk.byteVector(corrupted), schema)

    decoded shouldBe a[Left[String, Row]]
    decoded match {
      case Left(error) =>
        error should include("CRC mismatch")
      case Right(_) => fail("Should not decode successfully with corrupted CRC")
    }
  }

  test("decode should work without CRC when FieldType.CRC32 is not in schema") {
    val schema = List(
      FieldDef("valueSize", FieldType.Int32),
      FieldDef("value", FieldType.StringUtf8(sizeFromField = "valueSize"))
    )

    val row = Map(
      "valueSize" -> 5,
      "value"      -> "hello"
    )

    val encoded = encode(row, schema)
    val decoded = decode(encoded, schema)

    decoded shouldBe a[Right[String, Row]]
    decoded match {
      case Right(decodedRow) =>
        decodedRow("valueSize") should be(5)
        decodedRow("value") should be("hello")
      case Left(error) => fail(s"Should not fail with error: $error")
    }
  }

  test("CRC32Calculator should calculate consistent checksums") {
    val data1 = "hello world".getBytes("UTF-8")
    val data2 = "hello world".getBytes("UTF-8")

    val crc1 = CRC32Calculator.calculate(data1)
    val crc2 = CRC32Calculator.calculate(data2)

    crc1 should be(crc2)
    crc1 should be > 0L
  }

  test("CRC32Calculator should verify data integrity") {
    val data = "test data".getBytes("UTF-8")
    val crc = CRC32Calculator.calculate(data)

    val result = CRC32Calculator.verify(data, crc)

    result shouldBe a[CRCResult]
    result match {
      case CRCResult.Valid => succeed
      case CRCResult.Invalid(expected, actual) =>
        fail(s"Should be valid, but got invalid: expected $expected, actual $actual")
    }
  }

  test("CRC32Calculator should detect corruption") {
    val data = "test data".getBytes("UTF-8")
    val crc = CRC32Calculator.calculate(data)

    // Corrupt the data
    val corruptedData = "corrupted data".getBytes("UTF-8")

    val result = CRC32Calculator.verify(corruptedData, crc)

    result shouldBe a[CRCResult]
    result match {
      case CRCResult.Valid => fail("Should detect corruption")
      case CRCResult.Invalid(expected, actual) =>
        expected should be(crc)
        actual should not be crc
    }
  }
}
