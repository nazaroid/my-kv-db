package org.nazaroid.kvdb

import cats.effect.Async
import org.typelevel.log4cats.Logger

/**
 * Abstract database manager that handles multiple databases
 * This is the correct abstraction level - StorageManager handles single table
 */
trait DatabaseManager[F[_]] {
  
  def createDatabase(name: String): F[Database[F]]
  def getDatabase(name: String): F[Option[Database[F]]]
  def listDatabases: F[List[String]]
  def deleteDatabase(name: String): F[Unit]
  
  def getStats: F[DatabaseStats]
}

/**
 * Abstract database interface
 */
trait Database[F[_]] {
  def name: String
  def createTable(name: String): F[Unit]
  def getTable(name: String): F[Option[Table[F]]]
  def listTables: F[List[String]]
  def deleteTable(name: String): F[Unit]
}

/**
 * Abstract table interface
 */
trait Table[F[_]] {
  def name: String
  def get(key: String): F[Option[String]]
  def set(key: String, value: String): F[Unit]
  def delete(key: String): F[Unit]
  def listKeys: F[List[String]]
}

/**
 * Database statistics for multiple databases
 */
case class DatabaseStats(
  totalDatabases: Int,
  totalTables: Int,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  // Heterogeneous collection for engine-specific details
  details: Map[String, io.circe.Json] = Map.empty
)

/**
 * Database information for single database
 */
case class DatabaseInfo(
  name: String,
  totalTables: Int,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  // Engine-specific details
  details: Map[String, io.circe.Json] = Map.empty
)
