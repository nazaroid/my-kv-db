package org.nazaroid.kvdb.engine.bitcask

import cats.effect.implicits.given
import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.algebra.DbEngine
import org.nazaroid.kvdb.binfileio.{FieldDef, FieldType, StorageConfig}
import org.nazaroid.kvdb.bitcask.catalog.*

object BitcaskDbEngine {
  final case class State[F[_]: Async: Files](catalog: Ref[F, Catalog[F]])

  final case class Conf(
    rootDir:              String = "kvdb",
    fileWriteParallelism: Int = 10,
    fileWriteBufferSize:  Int = 10000,
    maxSegmentSize:       Int = 1024 * 10)
}

final class BitcaskDbEngine[F[_]: Async](conf: BitcaskDbEngine.Conf, state: BitcaskDbEngine.State[F])
    extends DbEngine[F] {

  override def init(): F[Unit] = {
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
    Catalog.init[F](Path("./my_storage"), storageConfig, conf.fileWriteBufferSize, conf.fileWriteParallelism).use {
      state.catalog.set
    }
  }

  override def createDbIfNotExists(name: String): F[Unit] = {
    for {
      c <- state.catalog.get
      _ <- c.database(name)
    } yield ()
  }

  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    for {
      c  <- state.catalog.get
      db <- c.database(baseName)
      _  <- db.table(tblName)
    } yield ()
  }

  override def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = {
    for {
      c    <- state.catalog.get
      db   <- c.database(baseName)
      tbl  <- db.table(tblName)
      vOpt <- tbl.read(key)
    } yield vOpt
  }

  override def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    for {
      c   <- state.catalog.get
      db  <- c.database(baseName)
      tbl <- db.table(tblName)
      _   <- tbl.write(key, value)
    } yield ()
  }

}
