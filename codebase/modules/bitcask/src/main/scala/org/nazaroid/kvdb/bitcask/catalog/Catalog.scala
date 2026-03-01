package org.nazaroid.kvdb.bitcask.catalog

import cats.effect.implicits.given
import cats.effect.{Async, Ref, Resource}
import cats.implicits.given
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{WriteTask, writeBinary}
import org.nazaroid.kvdb.bitcask.storage.{StorageConfig, StorageManager}
import org.typelevel.log4cats.Logger

final class Catalog[F[_]: Async: Files: Logger](
  val rootPath:       Path,
  val writeQueue:     Channel[F, WriteTask[F]],
  val configTemplate: StorageConfig,
  val databases:      Ref[F, Map[String, Database[F]]]) {

  def database(dbName: String): F[Database[F]] = {
    databases.get.flatMap { activeDbs =>
      activeDbs.get(dbName) match {
        case Some(db) => Async[F].pure(db)
        case None =>
          val dbPath = rootPath / dbName
          for {
            _         <- Files[F].createDirectories(dbPath).handleError(_ => ())
            tablesRef <- Ref.of[F, Map[String, StorageManager[F]]](Map.empty)
            db = new Database(dbName, dbPath, writeQueue, configTemplate, tablesRef)
            _ <- databases.update(_ + (dbName -> db))
          } yield db
      }
    }
  }
}

object Catalog {

  def init[F[_]: Async: Files: Logger](
    rootPath:       Path,
    configTemplate: StorageConfig,
    queueSize:      Int = 10000,
    parallelism:    Int = 10
  ): Resource[F, Catalog[F]] = {
    for {
      // 1. Create root directory if it doesn't exist
      _ <- Resource.eval(Files[F].createDirectories(rootPath).handleError(_ => ()))

      // 2. Create a unified write queue for the entire catalog
      writeQueue <- Channel.bounded[F, WriteTask[F]](queueSize).toResource

      // 3. Start background write worker (parallelism can be moved to config)
      // It will process tasks from all tables across all databases
      _ <- writeBinary(writeQueue.stream, parallelism = 1).compile.drain.background

      // 4. Initialize the registry for open databases
      activeDbs <- Resource.eval(Ref.of(Map.empty[String, Database[F]]))

    } yield Catalog[F](
      rootPath       = rootPath,
      writeQueue     = writeQueue,
      configTemplate = configTemplate,
      databases      = activeDbs
    )
  }
}
