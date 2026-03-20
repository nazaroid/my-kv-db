package org.nazaroid.kvdb.engine.examples

import cats.effect.{Async, Resource}
import cats.implicits.given
import org.nazaroid.kvdb.algebra.{Engine, DatabaseStats}
import org.typelevel.log4cats.Logger
import io.circe.syntax._

import scala.collection.mutable

/**
 * Example of in-memory engine with heterogeneous statistics
 */
class InMemoryEngine[F[_]: Async: Logger] extends Engine[F] {
  
  private val databases = mutable.Map[String, mutable.Map[String, String]]()
  
  override def createDbIfNotExists(name: String): F[Unit] = {
    Async[F].delay {
      databases.getOrElseUpdate(name, mutable.Map.empty)
    }
  }
  
  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    Async[F].delay {
      databases.get(baseName).foreach { db =>
        db.getOrElseUpdate(tblName, mutable.Map.empty)
      }
    }
  }
  
  override def get(baseName: String, tblName: String, key: String): F[Option[String]] = {
    Async[F].delay {
      databases.get(baseName).flatMap { db =>
        db.get(tblName).flatMap(_.get(key))
      }
    }
  }
  
  override def set(baseName: String, tblName: String, key: String, value: String): F[Unit] = {
    Async[F].delay {
      databases.get(baseName).foreach { db =>
        db.get(tblName).foreach(_.put(key, value))
      }
    }
  }
  
  override def delete(baseName: String, tblName: String, key: String): F[Unit] = {
    Async[F].delay {
      databases.get(baseName).foreach { db =>
        db.get(tblName).foreach(_.remove(key))
      }
    }
  }
  
  override def getStats: F[DatabaseStats] = {
    Async[F].delay {
      val totalTables = databases.size
      val totalEntries = databases.values.map(_.values.map(_.size).sum).sum
      val activeEntries = totalEntries // All entries are active in memory
      val deletedEntries = 0 // No deleted entries in memory
      val totalDataSize = totalEntries * 100 // Estimate 100 bytes per entry
      
      DatabaseStats(
        totalTables = totalTables,
        totalEntries = totalEntries,
        activeEntries = activeEntries,
        deletedEntries = deletedEntries,
        totalDataSize = totalDataSize,
        details = Map(
          "engine_type" -> "in_memory".asJson,
          "memory_usage_mb" -> (Runtime.getRuntime.totalMemory() / 1024 / 1024).asJson,
          "max_memory_mb" -> (Runtime.getRuntime.maxMemory() / 1024 / 1024).asJson,
          "database_count" -> totalTables.asJson,
          "average_entries_per_db" -> (if (totalTables > 0) totalEntries / totalTables else 0).asJson,
          "persistence" -> false.asJson,
          "compression" -> false.asJson
        )
      )
    }
  }
}

object InMemoryEngine {
  def create[F[_]: Async: Logger]: Resource[F, Engine[F]] = {
    Resource.pure(new InMemoryEngine[F]())
  }
}
