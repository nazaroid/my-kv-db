package org.nazaroid.kvdb.srv

import org.nazaroid.kvdb.bitcask.BitcaskEngineConfig
import org.nazaroid.kvdb.{EngineConfig, ServerConfig, ServerConfigBase}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait ServerConfigBase {
  val host: String
  val port: Int
}

enum ServerConfig extends ServerConfigBase:

  case Http(
    host:           String = "127.0.0.1",
    port:           Int = 9000,
    maxConnections: Int = 1024,
    idleTimeout:    FiniteDuration = 60.seconds)

  case Grpc(
    host: String,
    port: Int)


final case class DbInstanceConfig(
  server: ServerConfig = ServerConfig.Http(),
  engine: BitcaskEngineConfig = BitcaskEngineConfig())
