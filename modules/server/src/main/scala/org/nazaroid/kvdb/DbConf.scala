package org.nazaroid.kvdb

import scala.concurrent.duration.{DurationInt, FiniteDuration}

//noinspection ScalaStyle
final case class DbConf(
  server: DbSrvConf = DbSrvConf(),
  engine: DbEngineConf = DbEngineConf())

final case class DbSrvConf(
  http: HttpSrvConf = HttpSrvConf())

final case class DbEngineConf(
  bitcask: BitcaskConf = BitcaskConf())

final case class HttpSrvConf(
  host:           String = "127.0.0.1",
  port:           Int = 9000,
  maxConnections: Int = 1024,
  idleTimeout:    FiniteDuration = 60.seconds)

final case class BitcaskConf(rootDir: String = "./kvdb")
