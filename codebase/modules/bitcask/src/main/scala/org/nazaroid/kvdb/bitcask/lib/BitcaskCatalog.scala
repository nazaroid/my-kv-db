package org.nazaroid.kvdb.bitcask.lib

import cats.effect.implicits.given
import cats.effect.{Async, Ref, Resource}
import cats.implicits.given
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.binfileio.{WriteTask, writeBinary}
import org.nazaroid.kvdb.bitcask.lib.*
import org.typelevel.log4cats.Logger

final case class BitcaskCatalogConfig(
  rootPath:         String,
  tableConfig:      BitcaskTableConfig,
  writeBufferSize:  Int = 10000,
  writeParallelism: Int = 10)

final class BitcaskCatalog[F[_]: Async: Files: Logger](
  val rootPath:       Path,
  val writeQueue:     Channel[F, WriteTask[F]],
  val configTemplate: BitcaskTableConfig,
  val databases:      Ref[F, Map[String, BitcaskDatabase[F]]]) {

  def getDatabase(dbName: String): F[Option[BitcaskDatabase[F]]] = {
    databases.get.map(_.get(dbName))
  }

  def createDatabase(dbName: String): F[BitcaskDatabase[F]] =
    getDatabase(dbName).flatMap {
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

  def listDatabases: F[List[String]] = {
    for {
      rootDirExists <- Files[F].exists(rootPath)
      result <-
        if (rootDirExists) {
          for {
            entries <- Files[F]
              .list(rootPath)
              .evalFilter(Files[F].isDirectory)
              .map(_.fileName.toString)
              .compile
              .toList
          } yield entries
        } else {
          Async[F].pure(List.empty)
        }
    } yield result
  }

  /** Get catalog statistics by aggregating all database statistics */
  def getStats: F[BitcaskCatalogStats] = {
    for {
      databaseNames <- listDatabases
      allDatabaseStats <- databaseNames.flatTraverse { dbName =>
        getDatabase(dbName).flatMap {
          case Some(db) => db.getStats.map(List(_))
          case None     => Async[F].pure(Nil)
        }
      }

      // Aggregate statistics from all databases
      totalDatabases = allDatabaseStats.size
      totalTables = allDatabaseStats.map(_.totalTables).sum
      totalEntries = allDatabaseStats.map(_.totalEntries).sum
      activeEntries = allDatabaseStats.map(_.activeEntries).sum
      deletedEntries = allDatabaseStats.map(_.deletedEntries).sum
      totalDataSize = allDatabaseStats.map(_.totalDataSize).sum
      totalSegments = allDatabaseStats.map(_.totalSegments).sum
      activeSegments = allDatabaseStats.map(_.activeSegments).sum

    } yield BitcaskCatalogStats(
      totalDatabases = totalDatabases,
      totalTables    = totalTables,
      totalEntries   = totalEntries,
      activeEntries  = activeEntries,
      deletedEntries = deletedEntries,
      totalDataSize  = totalDataSize,
      totalSegments  = totalSegments,
      activeSegments = activeSegments,
      databaseStats  = allDatabaseStats
    )
  }

  /** Load existing databases and their tables from filesystem */
  def loadExistingDatabases: F[BitcaskCatalog[F]] = {
    for {
      _       <- Logger[F].info("Loading existing databases...")
      dbNames <- listDatabases
      _       <- Logger[F].info(s"Found databases: $dbNames")

      loadedDbs <- dbNames.traverse { dbName =>
        loadDatabase(dbName)
      }

      _ <- Logger[F].info(s"Successfully loaded ${loadedDbs.size} databases")
    } yield this
  }

  /** Load a single database and its tables */
  private def loadDatabase(dbName: String): F[BitcaskDatabase[F]] = {
    for {
      _ <- Logger[F].info(s"Loading database: $dbName")
      dbPath = rootPath / dbName

      // Check if database directory exists
      exists <- Files[F].exists(dbPath)
      _ <-
        if (!exists) {
          Logger[F].warn(s"Database directory $dbPath does not exist, skipping")
        } else Async[F].unit

      // Create database instance
      tablesRef <- Ref.of[F, Map[String, BitcaskTable[F]]](Map.empty)
      db = BitcaskDatabase(dbName, dbPath, writeQueue, configTemplate, tablesRef)

      // Load existing tables
      _ <- db.loadExistingTables

      // Register database
      _ <- databases.update(_ + (dbName -> db))

    } yield db
  }

  def deleteDatabase(dbName: String): F[Unit] = {
    getDatabase(dbName).flatMap {
      case Some(db) =>
        for {
          _ <- databases.update(_ - dbName)
          _ <- Files[F].deleteIfExists(db.path)
        } yield ()
      case None => Async[F].unit
    }
  }
}

object BitcaskCatalog {

  def init[F[_]: Async: Files: Logger](
    config: BitcaskCatalogConfig
  ): Resource[F, BitcaskCatalog[F]] = {
    val rootPath = Path(config.rootPath)
    for {
      _ <- Logger[F].info(f"catalog reading: $rootPath").toResource
      // 1. Create root directory if it doesn't exist
      _ <- Files[F].createDirectories(rootPath).handleError(_ => ()).toResource

      // 2. Create a unified write queue for the entire catalog
      writeQueue <- Channel.bounded[F, WriteTask[F]](config.writeBufferSize).toResource

      // 3. Start background write worker
      // It will process tasks from all tables across all databases
      _ <- writeBinary(writeQueue.stream, parallelism = config.writeParallelism).compile.drain.background

      // 4. Initialize the registry for open databases
      activeDbs <- Ref.of(Map.empty[String, BitcaskDatabase[F]]).toResource

      // 5. Load existing databases and tables
      catalog <- Resource.eval(
        BitcaskCatalog[F](
          rootPath       = rootPath,
          writeQueue     = writeQueue,
          configTemplate = config.tableConfig,
          databases      = activeDbs
        ).loadExistingDatabases
      )

    } yield catalog
  }
}
