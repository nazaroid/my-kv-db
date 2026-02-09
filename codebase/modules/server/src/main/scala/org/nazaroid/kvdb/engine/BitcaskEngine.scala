package org.nazaroid.kvdb.engine

import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits.given
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.EngineConfig
import org.nazaroid.kvdb.algebra.Engine
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType}
import org.nazaroid.kvdb.bitcask.catalog.*
import org.nazaroid.kvdb.bitcask.storage.*

object BitcaskEngine {

  def init[F[_]: Async: Files](conf: EngineConfig): Resource[F, Engine[F]] = {
    val storageConfig = StorageConfig(
      folder         = conf.rootDir,
      maxSegmentSize = conf.maxSegmentSize, // Маленький размер для теста ротации (1КБ)
      dataSchema = List(
        FieldDef("recordSize", FieldType.Int32),
        FieldDef("value", FieldType.StringUtf8(sizeFromField = "recordSize"))
      ),
      segmentSchema = List(
        FieldDef("keySize", FieldType.Int32),
        FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
        FieldDef("offset", FieldType.Int64)
      ),
      tableSchema = List(
        FieldDef("keySize", FieldType.Int32),
        FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
        FieldDef("segmentNameSize", FieldType.Int32),
        FieldDef("segmentName", FieldType.StringUtf8(sizeFromField = "segmentNameSize"))
      )
    )
    for {
      c <- Catalog.init(Path(conf.rootDir), storageConfig, conf.fileWriteBufferSize, conf.fileWriteParallelism)
    } yield BitcaskEngine(c)
  }
}

final class BitcaskEngine[F[_]: Async](c: Catalog[F]) extends Engine[F] {

  def createDbIfNotExists(name: String): F[Unit] = {
    for {
      _ <- c.database(name)
    } yield ()
  }

  def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    for {
      db <- c.database(baseName)
      _  <- db.table(tblName)
    } yield ()
  }

  def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = {
    for {
      db   <- c.database(baseName)
      tbl  <- db.table(tblName)
      vOpt <- tbl.read(key)
    } yield vOpt
  }

  def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    for {
      db  <- c.database(baseName)
      tbl <- db.table(tblName)
      _   <- tbl.write(key, value)
    } yield ()
  }
}
