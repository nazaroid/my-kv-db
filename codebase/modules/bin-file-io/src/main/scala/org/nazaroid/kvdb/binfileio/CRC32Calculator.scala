package org.nazaroid.kvdb.binfileio

import java.util.zip.CRC32

enum CRCResult {
  case Valid
  case Invalid(expected: Long, actual: Long)
  case Corrupted(message: String)
}

object CRC32Calculator {

  def calculate(data: Array[Byte]): Long = {
    val crc = new CRC32()
    crc.update(data)
    crc.getValue
  }

  def verify(data: Array[Byte], expectedCrc: Long): CRCResult = {
    try {
      val actualCrc = calculate(data)
      if (actualCrc == expectedCrc) CRCResult.Valid
      else CRCResult.Invalid(expectedCrc, actualCrc)
    } catch {
      case e: Exception => CRCResult.Corrupted(e.getMessage)
    }
  }
}
