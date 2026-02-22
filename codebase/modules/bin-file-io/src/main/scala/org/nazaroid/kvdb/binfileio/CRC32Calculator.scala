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
  
  def calculateWithLength(data: Array[Byte]): (Long, Int) = {
    val crc = new CRC32()
    crc.update(data)
    (crc.getValue, data.length)
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
  
  def verifyWithLength(data: Array[Byte], expectedCrc: Long, expectedLength: Int): CRCResult = {
    if (data.length != expectedLength) {
      CRCResult.Corrupted(s"Length mismatch: expected $expectedLength, actual ${data.length}")
    } else {
      verify(data, expectedCrc)
    }
  }
}
