package org.nazaroid.kvdb.bitcasklib.algebra

import cats.data.Kleisli.ask
import cats.effect.Async
import fs2.*
import org.nazaroid.kvdb.binfileio.*

import java.nio.file.{Files, Path}

trait FileService[F[_]: Async: fs2.io.file.Files] {

  def initFileService(): DbScript[F, Unit] =
    for {
      env             <- ask[F, Env[F]]
      fileWriteBuffer <- DbScript.lift(env.state.fileWriteBuffer.get)
      taskStream = Stream.fromQueueUnterminated(fileWriteBuffer)
      _ <- DbScript.lift(BinFileIO.write(input = taskStream, env.conf.fileWriteParallelism).compile.drain)
    } yield ()

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
    schema: List[FieldDef],
    row:    Row
  ): DbScript[F, Unit] = {
    for {
      env             <- ask[F, Env[F]]
      fileWriteBuffer <- DbScript.lift(env.state.fileWriteBuffer.get)
      _               <- DbScript.lift(fileWriteBuffer.offer(WriteTask(path.toString, schema, row)))
    } yield ()
  }
}
