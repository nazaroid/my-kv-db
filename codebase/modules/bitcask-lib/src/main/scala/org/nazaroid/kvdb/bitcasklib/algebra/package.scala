package org.nazaroid.kvdb.bitcasklib

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.concurrent.Channel
import org.nazaroid.kvdb.binfileio.*

import java.nio.file.{Path, Paths}

package object algebra {
  type Key = String
  type Value = String
  type BaseName = String
  type TblName = String
  type BaseRegistry[F[_]] = Map[BaseName, Base[F]]
  type BaseTables[F[_]] = Map[TblName, Tbl[F]]
  type DbScript[F[_], O] = Kleisli[F, Env[F], O]

  trait Env[F[_]: Async] {
    def conf: BitcaskConf

    def files: FileService[F]

    def base: BaseService[F]

    def tbl: TblService[F]
    
    def cache: CacheService[F]

    def state: State[F]
  }

  final case class DbCatalog()

  final case class Base[F[_]: Async](
    name:   BaseName,
    path:   Path,
    tables: Ref[F, BaseTables[F]])

  final case class Tbl[F[_]: Async](
    name:    TblName,
    path:    Path,
    storage: StorageManager[F])

  final case class State[F[_]: Async](
    registryRef:     Ref[F, BaseRegistry[F]],
    fileWriteBuffer: Ref[F, Channel[F, WriteTask[F]]])

  object Tbl {

    def create[F[_]: Async](base: Base[F], name: TblName): DbScript[F, Tbl[F]] = {
      val path = Paths.get(f"${base.path.toAbsolutePath}/$name")
      val config = StorageConfig(
        folder         = path.toString,
        maxSegmentSize = 1024, // Маленький размер для теста ротации (1КБ)
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
        env <- ask[F, Env[F]]
        storage <- DbScript.lift {
          for {
            writeQueue <- env.state.fileWriteBuffer.get
            storage    <- StorageManager.initialize(config = config, writeQueue = writeQueue)
          } yield storage
        }
      } yield Tbl(name, path, storage)
    }
  }

  object Base {

    def create[F[_]: Async](rootDir: String, name: BaseName): F[Base[F]] = {
      val path = Paths.get(f"${rootDir}/$name")
      for {
        tables <- Async[F].ref(Map.empty[TblName, Tbl[F]])
      } yield Base(name, path, tables)
    }
  }

  object DbScript {

    extension [F[_]: Async, O](s: DbScript[F, O])

      def run(using env: Env[F]): F[O] = {
        s.run(env)
      }

    extension [F[_]: Async, O](f: F[O])

      def lift: DbScript[F, O] = {
        Kleisli.liftK(f)
      }

  }
}
