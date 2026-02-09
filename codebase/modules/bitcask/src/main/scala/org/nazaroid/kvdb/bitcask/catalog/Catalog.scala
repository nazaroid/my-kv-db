package org.nazaroid.kvdb.bitcask.catalog

import cats.effect.implicits.given
import cats.effect.{Async, Ref, Resource}
import cats.implicits.given
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.*
import org.nazaroid.kvdb.bitcask.storage.*

final class Catalog[F[_]: Async: Files](
  val rootPath:       Path,
  val writeQueue:     Channel[F, WriteTask[F]],
  val configTemplate: StorageConfig,
  val databases:      Ref[F, Map[String, Database[F]]]) {

  /** Получить базу данных по имени */
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

  def init[F[_]: Async: Files](
    rootPath:       Path,
    configTemplate: StorageConfig,
    queueSize:      Int = 10000,
    parallelism: Int = 10
  ): Resource[F, Catalog[F]] = {
    for {
      // 1. Создаем корневую директорию, если её нет
      _ <- Resource.eval(Files[F].createDirectories(rootPath).handleError(_ => ()))

      // 2. Создаем единую очередь записи для всего каталога
      writeQueue <- Channel.bounded[F, WriteTask[F]](queueSize).toResource

      // 3. Запускаем фоновый воркер записи (параллелизм можно вынести в конфиг)
      // Он будет обрабатывать задачи от всех таблиц всех баз данных
      _ <- writeBinary(writeQueue.stream, parallelism = 1).compile.drain.background

      // 4. Инициализируем реестр открытых баз данных
      activeDbs <- Resource.eval(Ref.of(Map.empty[String, Database[F]]))

      catalog = new Catalog[F](
        rootPath = rootPath,
        writeQueue = writeQueue,
        configTemplate = configTemplate,
        databases = activeDbs
      )
    } yield catalog
  }
}
