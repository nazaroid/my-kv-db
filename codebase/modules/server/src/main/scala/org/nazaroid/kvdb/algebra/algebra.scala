package org.nazaroid.kvdb.algebra

import cats.effect.*

class DatabaseException extends Exception

trait DbServer[F[_]] {
  def run(stopSignal: Deferred[F, Unit]): F[Unit]
}

trait DbEngine[F[_]] {
  def init(): F[Unit]

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
}
