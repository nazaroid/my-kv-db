package org.nazaroid.kvdb.bitcask.lib

import cats.effect.Async
import cats.implicits.given
import org.nazaroid.kvdb.bitcask.lib.algebra.*

import java.nio.ByteBuffer
import java.nio.file.Path

package object bindata {

  object BinRecord {

    /*
        binary record format:
        [ record size | fld_1 size | fld_1 | fld_2 size | fld_2 | ... | fld_N size | fld_N ]
     */
    def read[F[_]: Async](
      path:               Path,
      offset:             Offset,
      recordSizeCapacity: Int = 4
    ): DbScript[F, FileRecord] = {
      val stream = new java.io.FileInputStream(path.toFile)
      val ch = stream.getChannel
      val bSize = ByteBuffer.allocate(recordSizeCapacity)
      ch.position(offset)
      ch.read(bSize)
      bSize.rewind()
      val recordSize = bSize.getInt
      val bRecord = ByteBuffer.allocate(recordSize)
      ch.read(bRecord)
      DbScript.lift(bRecord.array().pure[F])
    }
  }

  object TblIxRecord {
    private val recordSizeCapacity: Int = 4
    private val keySizeCapacity:    Int = 4
    private val sNameSizeCapacity:  Int = 4

    def create[F[_]: Async](key: Key, s: Segment[F]): F[FileRecord] = {
      val keyBytes = key.getBytes("UTF-8")
      val keySize = keyBytes.length
      val keySizeBytes = ByteBuffer.allocate(keySizeCapacity).putInt(keySize).array()
      val sNameBytes = s.name.getBytes
      val sNameSize = sNameBytes.length
      val sNameSizeBytes = ByteBuffer.allocate(sNameSizeCapacity).putInt(sNameSize).array()
      val recordSize = recordSizeCapacity + keySizeCapacity + keySize + sNameSize
      val recordSizeBytes = ByteBuffer
        .allocate(recordSizeCapacity)
        .putInt(recordSize)
        .array()

      (recordSizeBytes
        ++ keySizeBytes
        ++ keyBytes
        ++ sNameSizeBytes
        ++ sNameBytes).pure[F]
    }
  }

  object SegmentRecord {
    private val recordSizeCapacity = 4

    def create[F[_]: Async](value: Value): F[FileRecord] = {
      val valueBytes = value.getBytes("UTF-8")
      val valueSize = valueBytes.length
      val valueSizeBytes = ByteBuffer.allocate(recordSizeCapacity).putInt(valueSize).array()
      val recordSize = valueSize
      val recordSizeBytes = valueSizeBytes
      (recordSizeBytes ++ valueBytes).pure[F]
    }

    def getValue[F[_]: Async](r: FileRecord): F[Value] = {
      val value = new String(r, "UTF-8")
      value.pure[F]
    }
  }

  object SegmentIxRecord {
    private val keySizeCapacity:    Int = 4
    private val recordSizeCapacity: Int = 4
    private val offsetCapacity:     Int = 8

    def create[F[_]: Async](key: Key, offset: Offset): F[FileRecord] = {
      val keyBytes = key.getBytes("UTF-8")
      val keySize = keyBytes.length
      val keySizeBytes = ByteBuffer.allocate(keySizeCapacity).putInt(keySize).array()
      val offsetBytes = ByteBuffer.allocate(offsetCapacity).putLong(offset).array()
      val recordSize = keySizeCapacity + keySize + offsetCapacity
      val recordSizeBytes = ByteBuffer
        .allocate(recordSizeCapacity)
        .putInt(recordSize)
        .array()
      (recordSizeBytes
        ++ keySizeBytes
        ++ keyBytes
        ++ offsetBytes).pure[F]
    }
  }
}
