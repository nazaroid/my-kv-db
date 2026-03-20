package org.nazaroid.kvdb.database

/**
 * Abstract engine interface that works with DatabaseManager
 * This breaks circular dependency - engine depends on database module,
 * not on server module
 */
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
