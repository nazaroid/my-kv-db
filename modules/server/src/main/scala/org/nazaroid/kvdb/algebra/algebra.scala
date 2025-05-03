package org.nazaroid.kvdb.algebra

class DatabaseException extends Exception

final case class DbSrvConf(host: String = "127.0.0.1", port: Int = 9000)

trait DbServerFactory[F[_]] {
  def create(conf: DbSrvConf): F[DbServer[F]]
}

trait DbServer[F[_]] {
  def run(): F[DbServerHandle[F]]
}

trait DbServerHandle[F[_]] {
  def stop(): F[Unit]
}

trait DbEngine[F[_]] {
  def createDatabase(name: String): F[Database[F]]
}

trait Database[F[_]] {
  def createTable(name: String): F[Table[F]]
}

trait Table[F[_]] {
  def get(key: String):                F[String]
  def set(key: String, value: String): F[String]
}
