package org.nazaroid.kvdb

import scala.concurrent.duration.{DurationInt, FiniteDuration}

//noinspection ScalaStyle
final case class DbConf(
  host:           String = "127.0.0.1",
  port:           Int = 9000,
  maxConnections: Int = 1024,
  idleTimeout:    FiniteDuration = 60.seconds)
