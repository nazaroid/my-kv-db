package org.nazaroid.kvdb.bitcask

import cats.effect.implicits.given
import cats.effect.{Async, Ref, Resource}
import cats.implicits.given
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{WriteTask, writeBinary}
import org.typelevel.log4cats.Logger

final class Catalog[F[_]: Async: Files: Logger](
                                                 val rootPath:       Path,
                                                 val writeQueue:     Channel[F, WriteTask[F]],
                                                 val configTemplate: BitcaskTableConfig,
                                                 val databases:      Ref[F, Map[String, BitcaskDatabase[F]]]) {

  def database(dbName: String): F[BitcaskDatabase[F]] = {
    databases.get.flatMap { activeDbs =>
      activeDbs.get(dbName) match {
        case Some(db) => Async[F].pure(db)
        case None =>
          val dbPath = rootPath / dbName
          for {
            _         <- Files[F].createDirectories(dbPath).handleError(_ => ())
            tablesRef <- Ref.of[F, Map[String, BitcaskTable[F]]](Map.empty)
            db = new BitcaskDatabase(dbName, dbPath, writeQueue, configTemplate, tablesRef)
            _ <- databases.update(_ + (dbName -> db))
          } yield db
      }
    }
  }

  override def listDatabases: F[List[String]] = {
    for {
      rootDirExists <- Files[F].exists(rootPath)
      result <-
        if (rootDirExists) {
          for {
            entries <- Files[F]
              .list(rootPath)
              .filter(Files[F].isDirectory)
              .evalMap(entry => Files[F].fileName(entry))
              .compile
              .toList
          } yield entries
        } else {
          Async[F].pure(List.empty)
        }
    } yield result
  }
}

object Catalog {

  def init[F[_]: Async: Files: Logger](
                                        rootPath:       Path,
                                        configTemplate: BitcaskTableConfig,
                                        queueSize:      Int = 10000,
                                        parallelism:    Int = 10
  ): F[Catalog[F]] = {
    for {
      // 1. Create root directory if it doesn't exist
      _ <- Files[F].createDirectories(rootPath).handleError(_ => ())

      // 2. Create a unified write queue for the entire catalog
      writeQueue <- Channel.bounded[F, WriteTask[F]](queueSize).toResource

      // 3. Start background write worker (parallelism can be moved to config)
      // It will process tasks from all tables across all databases
      _ <- writeBinary(writeQueue.stream, parallelism = 1).compile.drain.background

      // 4. Initialize the registry for open databases
      activeDbs <- Ref.of(Map.empty[String, BitcaskDatabase[F]])

    } yield Catalog[F](
      rootPath       = rootPath,
      writeQueue     = writeQueue,
      configTemplate = configTemplate,
      databases      = activeDbs
    )
  }
}
