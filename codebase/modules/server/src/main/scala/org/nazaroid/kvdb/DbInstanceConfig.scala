package org.nazaroid.kvdb

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

final case class EngineConfig(
  rootDir:              String = "./testFolder",
  fileWriteParallelism: Int = 10,
  fileWriteBufferSize:  Int = 10000,
  maxSegmentSize:       Long = 1024 * 10,
  maxSegmentCount:      Int = 10)

final case class DbInstanceConfig(
  server: ServerConfig = ServerConfig.Http(),
  engine: EngineConfig = EngineConfig())
