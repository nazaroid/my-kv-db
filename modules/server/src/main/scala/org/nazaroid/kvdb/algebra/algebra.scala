package org.nazaroid.kvdb.algebra

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class DatabaseException extends Exception

//noinspection ScalaStyle
final case class DbSrvConf(
  host:           String = "127.0.0.1",
  port:           Int = 9000,
  maxConnections: Int = 1024,
  idleTimeout:    FiniteDuration = 60.seconds)

trait DbServerFactory[F[_]: DbRuntime] {
  def startSync(conf: DbSrvConf): Unit
  def startAsync(conf: DbSrvConf): Unit
}

trait DbRuntime[F[_]] {
  def shutdown(): Unit
}

trait DbServer[F[_]] {
  def run(): F[Unit]
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
