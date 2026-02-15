package org.nazaroid.kvdb.algebra

import cats.effect.Resource

trait Server[F[_]] {
  def run(): Resource[F, Unit]
}

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
}
