package org.nazaroid.kvdb.core

import cats.effect.Resource

trait Server[F[_]] {
  def run(): Resource[F, Unit]
}

/** Abstract engine interface for CRUD operations, statistics, and monitoring. */
trait Engine[F[_]] extends StatisticsService[F] {

  def createDbIfNotExists(name: String): F[Unit]

  def createTableIfNotExists(baseName: String, tblName: String): F[Unit]

  def get(
    baseName: String,
    tblName: String,
    key: String
  ): F[Option[String]]

  def set(
    baseName: String,
    tblName: String,
    key: String,
    value: String
  ): F[Unit]

  def delete(
    baseName: String,
    tblName: String,
    key: String
  ): F[Unit]

}

trait Catalog[F[_]] {
  def createDatabase(name: String): F[Database[F]]
  def getDatabase(name: String):    F[Option[Database[F]]]
  def deleteDatabase(name: String): F[Unit]
  def listDatabases:                F[List[String]]

  def getStats:                                         F[CatalogStats]
  def getDatabaseStats(dbName: String):                 F[Option[DatabaseInfo]]
  def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]]
}

trait Database[F[_]] {
  def name:                      String
  def createTable(name: String): F[Table[F]]
  def getTable(name: String):    F[Option[Table[F]]]
  def listTables:                F[List[String]]
  def deleteTable(name: String): F[Unit]
}

trait Table[F[_]] {
  def name:                            String
  def get(key: String):                F[Option[String]]
  def set(key: String, value: String): F[Unit]
  def delete(key: String):             F[Unit]
}
