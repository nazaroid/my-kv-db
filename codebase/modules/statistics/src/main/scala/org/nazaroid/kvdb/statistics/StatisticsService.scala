package org.nazaroid.kvdb.statistics

import cats.effect.implicits.given
import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.io.file.{Files, Path}
import io.circe.*
import org.nazaroid.kvdb.database.{DatabaseManager, DatabaseInfo, TableInfo, DatabaseStats, SegmentInfo}
import org.typelevel.log4cats.Logger

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Background process for monitoring segments and fragmentation */
trait StatisticsService[F[_]] {
  def startMonitoring():                F[Unit]
  def stopMonitoring():                 F[Unit]
  def getDatabases:                     F[List[DatabaseInfo]]
  def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]]
  def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]]
  def getStats:                         F[DatabaseStats]  // Delegate to databaseManager
  def registerMetrics():                F[Unit]
}

case class MonitoringConfig(
  checkInterval:              FiniteDuration = 30.seconds,
  enableBackgroundMonitoring: Boolean = true,
  maxStaleRatio:              Double = 0.3,
  compactionThreshold:        Double = 0.5)

class StatisticsServiceImpl[F[_]: Async: Files: Logger](
  databaseManager: DatabaseManager[F],  // ✅ Работаем с базами, не с таблицами
  config:         MonitoringConfig,
  monitoringRef:  Ref[F, Boolean],
  metricsAdapter: MetricsAdapter[F] // Injected via constructor, no Option!
) extends StatisticsService[F] {

  override def registerMetrics(): F[Unit] = {
    for {
      _ <- Logger[F].info("Registering metrics with adapter")
      _ <- metricsAdapter.registerDatabaseMetrics()
      _ <- metricsAdapter.registerTableMetrics()
      _ <- metricsAdapter.registerSegmentMetrics()
      // Initial update with current values
      _ <- updateAdapterMetrics()
    } yield ()
  }

  override def getStats: F[DatabaseStats] = {
    databaseManager.getStats
  }

  override def startMonitoring(): F[Unit] = {
    if (config.enableBackgroundMonitoring) {
      // Define the monitoring stream recursively to ensure it restarts after errors
      lazy val monitoringStream: Stream[F, Unit] = Stream
        .fixedRate[F](config.checkInterval)
        .evalMap(_ => collectStatistics())
        .handleErrorWith { error =>
          // Log the error, wait for backoff, and restart the stream
          val recovery = Stream.eval(
            Logger[F].error(s"Error in monitoring stream: $error") *>
              Async[F].sleep(5.seconds)
          )
          recovery >> monitoringStream
        }

      for {
        _ <- Logger[F].info("Starting statistics monitoring service")
        _ <- monitoringRef.set(true)
        // .start runs the stream in a background Fiber to prevent blocking the startup
        _ <- monitoringStream.compile.drain.start.void
      } yield ()
    } else {
      Logger[F].info("Background monitoring disabled")
    }
  }

  override def stopMonitoring(): F[Unit] = {
    Logger[F].info("Stopping statistics monitoring service")
    monitoringRef.set(false)
  }

  override def getDatabases: F[List[DatabaseInfo]] = {
    for {
      dbNames <- databaseManager.listDatabases
      dbInfos   <- dbNames.traverse(collectDatabaseInfo)
    } yield dbInfos.flatten
  }

  override def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] = {
    // Используем метод databaseManager.getDatabaseStats если доступен
    // Иначе собираем через getDatabases
    databaseManager match {
      case bdm: org.nazaroid.kvdb.bitcask.BitcaskDatabaseManager[F] =>
        bdm.getDatabaseStats(dbName)
      case _ =>
        getDatabases.map(_.find(_.name == dbName))
    }
  }

  override def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]] = {
    // Используем метод databaseManager.getTableStats если доступен
    databaseManager match {
      case bdm: org.nazaroid.kvdb.bitcask.BitcaskDatabaseManager[F] =>
        bdm.getTableStats(dbName, tableName)
      case _ =>
        // Fallback: собираем через getDatabaseStats
        getDatabaseStats(dbName).map(_.flatMap { dbInfo =>
          dbInfo.details.get("tables").flatMap { tablesJson =>
            tablesJson.asArray.flatMap(_.toList.find(_.asObject.exists(_.apply("name").contains(tableName.asJson)))).map { tableJson =>
              val tableObj = tableJson.asObject.get
              TableInfo(
                name = tableName,
                totalEntries = tableObj("total_entries").flatMap(_.asNumber).map(_.toInt).getOrElse(0),
                activeEntries = tableObj("active_entries").flatMap(_.asNumber).map(_.toInt).getOrElse(0),
                deletedEntries = tableObj("deleted_entries").flatMap(_.asNumber).map(_.toInt).getOrElse(0),
                totalDataSize = tableObj("total_data_size").flatMap(_.asNumber).map(_.toLong).getOrElse(0L),
                details = tableObj.toMap.filterKeys(_ != "name" && _ != "total_entries" && _ != "active_entries" && _ != "deleted_entries" && _ != "total_data_size")
              )
            }
          }
        })
    }
  }

  /** Collect statistics for all databases */
  private def collectStatistics(): F[Unit] = {
    for {
      databases <- getDatabases
      _ <- databases.traverse_ { db =>
        for {
          _ <-
            if (db.details.get("fragmentation_ratio").exists(_.asNumber.exists(_.toDouble > config.maxStaleRatio))) {
              Logger[F].warn(s"Database ${db.name} has high fragmentation")
            } else ().pure[F]

          // Проверяем сегменты через детали базы данных
          segments <- db.details.get("tables") match {
            case Some(tablesJson) =>
              tablesJson.asArray.map(_.toList.flatMap { tableJson =>
                tableJson.asObject.flatMap { tableObj =>
                  tableObj("segments").asArray.map(_.toList.map { segmentJson =>
                    val segmentObj = segmentJson.asObject.get
                    SegmentInfo(
                      name = segmentObj("name").flatMap(_.asString).getOrElse("unknown"),
                      filePath = "", // Не доступно в статистике
                      fileSize = segmentObj("file_size").flatMap(_.asNumber).map(_.toLong).getOrElse(0L),
                      isActive = segmentObj("is_active").flatMap(_.asBoolean).getOrElse(false),
                      staleDataRatio = segmentObj("stale_data_ratio").flatMap(_.asNumber).map(_.toDouble).getOrElse(0.0),
                      entryCount = segmentObj("entry_count").flatMap(_.asNumber).map(_.toInt).getOrElse(0),
                      lastModified = 0L // Не доступно в статистике
                    )
                  })
                }
              }).getOrElse(List.empty)
            case None => List.empty
          }
          
          _ <- segments.traverse_ { segment =>
            if (!segment.isActive && segment.staleDataRatio > config.compactionThreshold) {
              Logger[F].info(s"Segment ${segment.name} in database ${db.name} needs compaction")
            } else ().pure[F]
          }
        } yield ()
      }
      _ <- updateAdapterMetrics()
    } yield ()
  }

  /** Collect database information using databaseManager */
  private def collectDatabaseInfo(dbName: String): F[Option[DatabaseInfo]] = {
    getDatabaseStats(dbName)
  }
  
  /** Update metrics through adapter */
  private def updateAdapterMetrics(): F[Unit] = {
    for {
      databases <- getDatabases
      _         <- metricsAdapter.updateDatabaseMetrics(databases)
      _         <- metricsAdapter.updateTableMetrics(databases)
      _         <- metricsAdapter.updateSegmentMetrics(databases)
    } yield ()
  }
}

object StatisticsService {

  def create[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:         MonitoringConfig = MonitoringConfig()
  ): F[StatisticsService[F]] = {
    createWithAdapter(databaseManager, config, MetricsAdapter.createNoOpAdapter(using summon[Async[F]]))
  }

  def createWithPrometheus[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:            MonitoringConfig = MonitoringConfig(),
    collectorRegistry: io.prometheus.client.CollectorRegistry
  ): F[StatisticsService[F]] = {
    val prometheusAdapter = MetricsAdapter.createPrometheusAdapter(collectorRegistry)
    for {
      service <- createWithAdapter(databaseManager, config, prometheusAdapter)
    } yield service
  }

  def createWithAdapter[F[_]: Async: Files: Logger](
    databaseManager: DatabaseManager[F],
    config:         MonitoringConfig,
    metricsAdapter: MetricsAdapter[F]
  ): F[StatisticsService[F]] = {
    for {
      monitoringRef <- Ref.of[F, Boolean](false)
      service = new StatisticsServiceImpl(databaseManager, config, monitoringRef, metricsAdapter)
    } yield service
  }
}
