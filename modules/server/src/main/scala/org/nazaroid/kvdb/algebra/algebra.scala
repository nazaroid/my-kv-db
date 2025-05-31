package org.nazaroid.kvdb.algebra

class DatabaseException extends Exception

trait DbServer[F[_]] {
  def run(): F[Unit]
}

trait DbEngine[F[_]] {
  def createDatabase(name: String): F[Unit]
  def getDatabase(name: String):    F[Database[F]]
}

trait Database[F[_]] {
  def createTable(name: String): F[Unit]
  def getTable(name: String):    F[Table[F]]
}

trait Table[F[_]] {
  def get(key: String):                F[String]
  def set(key: String, value: String): F[Unit]
}
