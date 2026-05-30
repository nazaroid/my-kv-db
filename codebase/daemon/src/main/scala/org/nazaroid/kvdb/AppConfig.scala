package org.nazaroid.kvdb

import org.nazaroid.kvdb.srv.DbInstanceConfig
import pureconfig.*
import pureconfig.generic.derivation.default.*

final case class AppConfig(
  metricsPort:    Int = 9091,
  metricsEnabled: Boolean = true,
  dbConf:         DbInstanceConfig = DbInstanceConfig())
    derives ConfigReader {}

object AppConfig {
  val appName: String = "my-kv-db"
}
