package org.nazaroid.kvdb.algebra

import cats.effect.Resource
import io.circe.generic.auto._
import io.circe.Json
import io.circe.syntax._

// Abstract database statistics with heterogeneous details
case class DatabaseStats(
  totalTables: Int,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  // Heterogeneous collection for engine-specific details
  details: Map[String, Json] = Map.empty
)

// Common abstract types that most engines might use
case class TableStats(
  name: String,
  entryCount: Int,
  activeEntryCount: Int,
  // Engine-specific details
  details: Map[String, Json] = Map.empty
)

// Common abstract types for storage-like engines
case class SegmentStats(
  name: String,
  fileSize: Long,
  isActive: Boolean,
  staleDataRatio: Double,
  entryCount: Int,
  // Engine-specific details
  details: Map[String, Json] = Map.empty
)

// Abstract database info
case class DatabaseInfo(
  name: String,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  // Engine-specific details
  details: Map[String, Json] = Map.empty
)

// Abstract segment info
case class SegmentInfo(
  name: String,
  fileSize: Long,
  isActive: Boolean,
  entryCount: Int,
  // Engine-specific details
  details: Map[String, Json] = Map.empty
)

trait Server[F[_]] {
  def run(): Resource[F, Unit]
}

trait Engine[F[_]] {

  def createDbIfNotExists(name: String): F[Unit]

  def createTableIfNotExists(baseName: String, tblName: String): F[Unit]

  def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]]

  def set(
    baseName: String,
    tblName: String,
    key:      String,
    value:    String
  ): F[Unit]

  def delete(
    baseName: String,
    tblName: String,
    key:      String
  ): F[Unit]
  
  def getStats: F[DatabaseStats]
}
