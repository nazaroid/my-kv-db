package org.nazaroid.kvdb

import org.nazaroid.kvdb.algebra.DbSrvConf
import pureconfig.*
import pureconfig.generic.derivation.default.*

final case class AppConfig(
  metricsPort:    Int = 9091,
  metricsEnabled: Boolean = true,
  dbSrvConf:      DbSrvConf = DbSrvConf())
    derives ConfigReader {}

object AppConfig {
  val appName: String = "my-kv-db"
}
