package org.nazaroid.kvdb.statistics

import cats.effect.{Async, Resource}
import cats.implicits.given
import org.nazaroid.kvdb.bitcask.storage.StorageManager
import org.typelevel.log4cats.Logger

/** Integration layer for statistics with BitcaskEngine */
class StatisticsIntegration[F[_]: Async: Logger](
  statisticsService: StatisticsService[F]
) {

  /** Start background monitoring */
  def startMonitoring(): F[Unit] = {
    Logger[F].info("Starting statistics integration monitoring")
    statisticsService.startMonitoring()
  }

  /** Stop background monitoring */
  def stopMonitoring(): F[Unit] = {
    Logger[F].info("Stopping statistics integration monitoring")
    statisticsService.stopMonitoring()
  }

  /** Get all databases with their statistics */
  def getAllDatabases(): F[List[DatabaseInfo]] = {
    statisticsService.getDatabases
  }

  /** Get specific database statistics */
  def getDatabaseStats(dbName: String): F[Option[DatabaseInfo]] = {
    statisticsService.getDatabaseStats(dbName)
  }

  /** Get segment statistics for a database */
  def getSegmentStats(dbName: String): F[List[SegmentInfo]] = {
    statisticsService.getSegmentStats(dbName)
  }

  /** Register statistics metrics with Prometheus collector registry */
  def registerMetrics(collectorRegistry: io.prometheus.client.CollectorRegistry): F[Unit] = {
    Logger[F].info("Registering statistics metrics with Prometheus collector registry") *>
    statisticsService.registerMetrics(collectorRegistry)
  }

  /** Export statistics for Prometheus (legacy method for compatibility) */
  def exportForPrometheus(): F[String] = {
    // This method is kept for compatibility but registerMetrics should be used instead
    for {
      databases <- statisticsService.getDatabases
      prometheusLines = databases.flatMap { db =>
        val dbLines = List(
          s"# HELP kvdb_database_info Database information",
          s"# TYPE kvdb_database_info gauge",
          s"kvdb_database_info{database=\"${db.name}\",type=\"total_entries\"} ${db.totalEntries}",
          s"kvdb_database_info{database=\"${db.name}\",type=\"active_entries\"} ${db.activeEntries}",
          s"kvdb_database_info{database=\"${db.name}\",type=\"deleted_entries\"} ${db.deletedEntries}",
          s"kvdb_database_info{database=\"${db.name}\",type=\"disk_size_bytes\"} ${db.totalDiskSize}",
          s"kvdb_database_info{database=\"${db.name}\",type=\"memory_size_bytes\"} ${db.totalMemorySize}",
          s"kvdb_database_info{database=\"${db.name}\",type=\"fragmentation_ratio\"} ${db.fragmentationRatio}"
        )
        
        val tableLines = db.tables.flatMap { table =>
          List(
            s"kvdb_table_info{database=\"${db.name}\",table=\"${table.name}\",type=\"entries\"} ${table.entryCount}",
            s"kvdb_table_info{database=\"${db.name}\",table=\"${table.name}\",type=\"active_entries\"} ${table.activeEntryCount}",
            s"kvdb_table_info{database=\"${db.name}\",table=\"${table.name}\",type=\"disk_size_bytes\"} ${table.diskSize}",
            s"kvdb_table_info{database=\"${db.name}\",table=\"${table.name}\",type=\"memory_size_bytes\"} ${table.memorySize}"
          )
        }
        
        dbLines ++ tableLines
      }
      
      segmentLines <- databases.traverse { db =>
        statisticsService.getSegmentStats(db.name).map { segments =>
          segments.flatMap { segment =>
            List(
              s"kvdb_segment_info{database=\"${db.name}\",segment=\"${segment.name}\",type=\"file_size_bytes\"} ${segment.fileSize}",
              s"kvdb_segment_info{database=\"${db.name}\",segment=\"${segment.name}\",type=\"is_active\"} ${if (segment.isActive) 1 else 0}",
              s"kvdb_segment_info{database=\"${db.name}\",segment=\"${segment.name}\",type=\"stale_ratio\"} ${segment.staleDataRatio}",
              s"kvdb_segment_info{database=\"${db.name}\",segment=\"${segment.name}\",type=\"entry_count\"} ${segment.entryCount}"
            )
          }
        }
      }.map(_.flatten)
      
      allLines = prometheusLines ++ segmentLines
      
    } yield allLines.mkString("\n")
  }

  /** Get health check information */
  def getHealthCheck(): F[HealthStatus] = {
    for {
      databases <- statisticsService.getDatabases
      totalDbs = databases.size
      healthyDbs = databases.count(_.fragmentationRatio < 0.5)
      avgFragmentation = if (totalDbs > 0) {
        databases.map(_.fragmentationRatio).sum / totalDbs
      } else 0.0
      
      healthStatus = if (healthyDbs == totalDbs && avgFragmentation < 0.3) {
        HealthStatus.Healthy
      } else if (healthyDbs > 0) {
        HealthStatus.Degraded
      } else {
        HealthStatus.Unhealthy
      }
      
    } yield HealthStatus(
      status = healthStatus,
      totalDatabases = totalDbs,
      healthyDatabases = healthyDbs,
      averageFragmentation = avgFragmentation,
      timestamp = System.currentTimeMillis()
    )
  }
}

case class HealthStatus(
  status: HealthStatus.Status,
  totalDatabases: Int,
  healthyDatabases: Int,
  averageFragmentation: Double,
  timestamp: Long
)

object HealthStatus {
  sealed trait Status
  case object Healthy extends Status
  case object Degraded extends Status  
  case object Unhealthy extends Status
}

object StatisticsIntegration {
  def create[F[_]: Async: Logger](
    storageManager: StorageManager[F],
    monitoringConfig: MonitoringConfig = MonitoringConfig()
  ): F[StatisticsIntegration[F]] = {
    for {
      statisticsService <- StatisticsService.create(storageManager, monitoringConfig)
      integration = new StatisticsIntegration(statisticsService)
    } yield integration
  }
}
