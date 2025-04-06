package com.uzumdata.cc.api.algebra

import io.circe.JsonObject

import java.util.UUID

trait Engine[F[_]] {
  def run(): F[Unit]
}

trait FeatureDbMigrator[F[_]] {
  def migrateOrExit(): F[Unit]
}

trait FeatureDb[F[_]] {
  def switchTable(): F[Unit]

  def runSwitchTableLoop(): F[Unit]

  def retrieveColumnNames(): F[Set[String]]

  def retrieveByAccountId(accountId: Long, columnSelector: FeatureDb.DbColumnSet): F[Option[JsonObject]]

  def retrieveByUzumId(uzumId: UUID, columnSelector: FeatureDb.DbColumnSet): F[Option[JsonObject]]

  def retrieveByMsisdnHash(msisdn_hash: String, columnSelector: FeatureDb.DbColumnSet): F[Option[JsonObject]]

  def retrieveByUbankId(ubankId: Int, columnSelector: FeatureDb.DbColumnSet): F[Option[JsonObject]]

  def retrieveByNasiyaId(nasiyaId: Int, columnSelector: FeatureDb.DbColumnSet): F[Option[JsonObject]]

  def retrieveByTezkorId(tezkorId: UUID, columnSelector: FeatureDb.DbColumnSet): F[Option[JsonObject]]
}

object FeatureDb {
  type DbColumnSet = Map[String, DbColumnType]

  type DbColumnType = String

  object DbColumnTypes {
    val `text`    = "text"
    val `uuid`    = "uuid"
    val `int`     = "int"
    val `bigint`  = "bigint"
    val `boolean` = "boolean"
    val `double`  = "double"
  }
}

type CredentialData = Map[String, String]

trait CredentialsSource[F[_]] {
  def isStatic: Boolean
  def get():    F[CredentialData]
}

trait UserCredsValidator[F[_]] {
  def refreshCreds():                      F[Unit]
  def runRefreshCredsLoop():               F[Unit]
  def isValid(login: String, pwd: String): F[Boolean]
}

object FeatureSpec {
  type FeatureSetDef = Map[String, FeatureDef]

  final case class FeatureDef(obfuscated_name: Option[String], db_type: FeatureDb.DbColumnType)
}

trait FeatureSpecAccessor[F[_]] {
  def get(): F[FeatureSpec.FeatureSetDef]
}

trait HotFeatureSpecAccessor[F[_]] extends FeatureSpecAccessor[F] {
  def runRefreshLoop(): F[Unit]
  def refresh(): F[Unit]
}

