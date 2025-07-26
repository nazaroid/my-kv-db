package org.nazaroid.kvdb.bitcask.lib.algebra

import cats.effect.Async

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

}
