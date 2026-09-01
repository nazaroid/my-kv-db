package org.nazaroid.kvdb.core

import cats.data.OptionT
import cats.effect.Async
import cats.effect.implicits.clockOps
import cats.implicits.given
import org.typelevel.log4cats.Logger

/** Default Engine implementation composed from Catalog, StatisticsService, and operation metrics. */
final class CatalogEngine[F[_]: Async: Logger](
  catalog:           Catalog[F],
  statisticsService: StatisticsService[F],
  metricRecorder:    PerformanceMetricRecorder[F])
    extends Engine[F] {

  override def startMonitoring(): F[Unit] = statisticsService.startMonitoring()

  override def stopMonitoring(): F[Unit] = statisticsService.stopMonitoring()

  override def getDatabases: F[List[DatabaseInfo]] = statisticsService.getDatabases

  override def getStats: F[CatalogStats] = statisticsService.getStats

  override def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] =
    statisticsService.getDatabaseStats(dbName)

  override def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]] =
    statisticsService.getTableStats(dbName, tableName)

  override def createDbIfNotExists(name: String): F[Unit] = {
    catalog.createDatabase(name).void
  }

  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    for {
      db <- OptionT(catalog.getDatabase(baseName))
        .getOrElseF(catalog.createDatabase(baseName))
      _ <- OptionT(db.getTable(tblName)).getOrElseF(db.createTable(tblName))
    } yield ()
  }

  override def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] =
    (for {
      db  <- OptionT(catalog.getDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName))
      v   <- OptionT(tbl.get(key))
    } yield v)
      .value
      .timed
      .flatTap { (duration, result) =>
        metricRecorder.recordGetOperation(duration)
      }
      .map { (duration, result) => result }

  override def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] =
    catalog
      .getDatabase(baseName)
      .flatMap(_.fold(catalog.createDatabase(baseName))(_.pure[F]))
      .flatMap { db =>
        db.getTable(tblName).flatMap(_.fold(db.createTable(tblName))(_.pure[F]))
      }
      .flatMap(_.set(key, value))
      .timed
      .flatTap { (duration, result) =>
        metricRecorder.recordSetOperation(duration)
      }
      .map { (duration, result) => result }

  override def delete(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Unit] =
    (for {
      db  <- OptionT(catalog.getDatabase(baseName))
      tbl <- OptionT(db.getTable(tblName))
      _   <- OptionT.liftF(tbl.delete(key))
    } yield ())
      .value
      .void
      .timed
      .flatTap { (duration, result) =>
        metricRecorder.recordDeleteOperation(duration)
      }
      .map { (duration, result) => result }

}
