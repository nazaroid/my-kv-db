package org.nazaroid.kvdb.bitcask.lib.algebra

import cats.effect.Async
import cats.implicits.given

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, StandardOpenOption}

trait FileService[F[_]: Async] {

  def createDirIfNotExists(path: Path): DbScript[F, Path] = {
    DbScript.lift {
      Async[F].blocking(Files.createDirectories(path))
    }
  }

  def createFile(path: Path): DbScript[F, Path] = {
    DbScript.lift {
      Async[F].blocking {
        if (Files.notExists(path)) {
          Files.createFile(path)
        } else path
      }
    }
  }

  def appendToFile(
    path:   Path,
    record: FileRecord
  ): DbScript[F, Unit] = {
    DbScript.lift {
      Async[F].blocking(Files.write(path, record, StandardOpenOption.CREATE, StandardOpenOption.APPEND))
    }
  }

  /*
    binary record format:
    [ record size | fld_1 size | fld_1 | fld_2 size | fld_2 | ... | fld_N size | fld_N ]
   */
  def readFileRecord(
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
