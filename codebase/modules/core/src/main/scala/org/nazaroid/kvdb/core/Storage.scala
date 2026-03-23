package org.nazaroid.kvdb.core

import cats.effect.Resource

trait Server[F[_]] {
  def run(): Resource[F, Unit]
}

/** Abstract engine interface that works with DatabaseManager This breaks circular dependency - engine depends on
  * database module, not on server module
  */
trait Engine[F[_]] {

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

/** Abstract database manager that handles multiple databases This module breaks circular dependencies between server
  * and statistics
  */
trait DatabaseManager[F[_]] {
  def createDatabase(name: String): F[Database[F]]
  def getDatabase(name: String):    F[Option[Database[F]]]
  def deleteDatabase(name: String): F[Unit]
  def listDatabases:                F[List[String]]

  def getStats:                                         F[CatalogStats]
  def getDatabaseStats(dbName: String):                 F[Option[DatabaseInfo]]
  def getTableStats(dbName: String, tableName: String): F[Option[TableInfo]]
}

/** Abstract database interface
  */
trait Database[F[_]] {
  def name:                      String
  def createTable(name: String): F[Table[F]]
  def getTable(name: String):    F[Option[Table[F]]]
  def listTables:                F[List[String]]
  def deleteTable(name: String): F[Unit]
}

/** Abstract table interface
  */
trait Table[F[_]] {
  def name:                            String
  def get(key: String):                F[Option[String]]
  def set(key: String, value: String): F[Unit]
  def delete(key: String):             F[Unit]
}
