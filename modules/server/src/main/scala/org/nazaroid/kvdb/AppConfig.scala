package com.uzumdata.cc.api

import com.uzumdata.cc.api.common.scylla.ScyllaConnConfig
import com.uzumdata.cc.pg.PgConnConfig
import pureconfig.*
import pureconfig.generic.derivation.default.*

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class FeatureReadingConfig(
  conn:        ScyllaConnConfig = ScyllaConnConfig(),
  keyspace:    String = "customer_catalog",
  tableSwitch: TableSwitchConfig = TableSwitchConfig())
    derives ConfigReader

final case class TableSwitchConfig(checkInterval: FiniteDuration = 1.minute) derives ConfigReader

final case class FeatureSpecReadingConfig(location: String = "jar://customer_profile_spec.json", refreshInterval: FiniteDuration = 1.minute) derives ConfigReader

// noinspection ScalaStyle
final case class ApiHttpEndpoint(
  host:           String = "127.0.0.1",
  port:           Int = 9000,
  maxConnections: Int = 1024,
  idleTimeout:    FiniteDuration = 60 seconds)
    derives ConfigReader

final case class AuthConfig(credsData: String = "", dbCreds: DbCredsConfig = DbCredsConfig()) derives ConfigReader {}

final case class DbCredsConfig(conn: PgConnConfig = PgConnConfig(), refreshInterval: FiniteDuration = 1 minute)
    derives ConfigReader

// noinspection ScalaStyle
final case class LoggingConfig(
  warnLatencyThreshold: Long = 5000)
    derives ConfigReader

// noinspection ScalaStyle
final case class AppConfig(
  metricsPort:         Int = 9091,
  metricsEnabled:      Boolean = true,
  apiHttpEndpoint:     ApiHttpEndpoint = ApiHttpEndpoint(),
  auth:                AuthConfig = AuthConfig(),
  adminDbEnabled:      Boolean = false,
  featureSpecReading:  FeatureSpecReadingConfig = FeatureSpecReadingConfig(),
  featureReading:      FeatureReadingConfig = FeatureReadingConfig(),
  nonObfuscatedLogins: String = "ui;test",
  logging:             LoggingConfig = LoggingConfig())
    derives ConfigReader {
  def parseNonObfuscatedLogins: Array[String] = nonObfuscatedLogins.split(";")
}

object AppConfig {
  val appName = "cc-api-app"
}
