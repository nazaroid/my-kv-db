package org.nazaroid.kvdb.engine.bitcask

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.effect.{Async, Ref}
import cats.implicits.given
import org.nazaroid.kvdb.BitcaskConf

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

object BitcaskLib {

  import algebra.*
  import instances.*

  def apply[F[_]: Async](c: BitcaskConf, s: State[F]): LibScenarios[F] = new LibScenariosImpl(c, s)

  def createState[F[_]: Async](): F[State[F]] = Async[F].ref(Map.empty[BaseName, Base[F]]) >>= { r => State(r).pure[F] }

  object instances {

    final class Dsl[F[_]: Async]
        extends FileService[F]
        with CacheService[F]
        with BaseService[F]
        with TblService[F]
        with SegmentService[F]
        with TblIxService[F]
        with SegmentIxService[F] {}

    final class LibScenariosImpl[F[_]: Async](c: BitcaskConf, s: State[F]) extends LibScenarios[F]:
      override def env: Env[F] = EnvImpl(c, s)

    final class EnvImpl[F[_]: Async](c: BitcaskConf, s: State[F]) extends Env[F]:
      private val dsl = new Dsl()

      override def conf: BitcaskConf = c

      override def files: FileService[F] = dsl

      override def base: BaseService[F] = dsl

      override def tbl: TblService[F] = dsl

      override def segment: SegmentService[F] = dsl

      override def tblIx: TblIxService[F] = dsl

      override def segmentIx: SegmentIxService[F] = dsl

      override def cache: CacheService[F] = dsl

      override def state: State[F] = s
  }

  object algebra {
    type Offset = Long
    type Key = String
    type Value = String
    type BaseName = String
    type TblName = String
    type TblIxName = String
    type SegmentNum = Int
    type SegmentName = String
    type SegmentIxName = String
    type SegmentIxData = Map[Key, Offset]
    type TblIxData[F[_]] = Map[Key, Segment[F]]
    type BaseRegistry[F[_]] = Map[BaseName, Base[F]]
    type BaseTables[F[_]] = Map[TblName, Tbl[F]]
    type DbScript[F[_], O] = Kleisli[F, Env[F], O]
    type FileRecord = Array[Byte]

    trait Env[F[_]: Async] {
      def conf: BitcaskConf

      def files: FileService[F]

      def base: BaseService[F]

      def tbl: TblService[F]

      def segment: SegmentService[F]

      def tblIx: TblIxService[F]

      def segmentIx: SegmentIxService[F]

      def cache: CacheService[F]

      def state: State[F]
    }

    trait CacheService[F[_]: Async] {

      def addBase(base: Base[F]): DbScript[F, BaseRegistry[F]] = {
        for {
          env <- ask[F, Env[F]]
          reg <- DbScript.lift(env.state.registryRef.updateAndGet(_.updated(base.name, base)))
        } yield reg
      }

      def addTbl(base: Base[F], tbl: Tbl[F]): DbScript[F, Base[F]] = {
        for {
          _ <- DbScript.lift(base.tables.update(_.updated(tbl.name, tbl)))
        } yield base
      }

      def getTbl(base: Base[F], tblName: TblName): DbScript[F, Tbl[F]] = {
        for {
          tbl <- DbScript.lift(base.tables.get).map(_(tblName))
        } yield tbl
      }

      def getBase(baseName: BaseName): DbScript[F, Base[F]] = {
        for {
          env  <- ask[F, Env[F]]
          base <- DbScript.lift(env.state.registryRef.get).map(_(baseName))
        } yield base
      }

      def addTblIx(tbl: Tbl[F], ix: TblIx[F]): DbScript[F, Tbl[F]] = {
        for {
          _ <- DbScript.lift(tbl.ix.set(ix.some))
        } yield tbl
      }

      def getTblIx(tbl: Tbl[F]): DbScript[F, Option[TblIx[F]]] = DbScript.lift(tbl.ix.get)

      def addSegmentIx(
        s:  Segment[F],
        ix: SegmentIx[F]
      ): DbScript[F, Segment[F]] = {
        for {
          _ <- DbScript.lift(s.ix.set(ix.some))
        } yield s
      }

      def getSegmentIx(s: Segment[F]): DbScript[F, Option[SegmentIx[F]]] = DbScript.lift(s.ix.get)

      def addSegment(
        tbl: Tbl[F],
        s:   Segment[F]
      ): DbScript[F, Tbl[F]] = {
        for {
          _ <- DbScript.lift(tbl.lastSegment.set(s.some))
        } yield tbl
      }

      def getSegmentOffset(s: Segment[F]): DbScript[F, Offset] = DbScript.lift(s.offset.get)

      def getLastSegment(tbl: Tbl[F]): DbScript[F, Option[Segment[F]]] = DbScript.lift(tbl.lastSegment.get)

      def findSegment(
        tblIx: TblIx[F],
        k:     Key
      ): DbScript[F, Option[Segment[F]]] = {
        for {
          env  <- ask[F, Env[F]]
          sOpt <- DbScript.lift(tblIx.data.get).map(_.get(k))
        } yield sOpt
      }

      def getOffsetBySegmentIx(sIx: SegmentIx[F], k: Key): DbScript[F, Offset] = {
        for {
          env    <- ask[F, Env[F]]
          offset <- DbScript.lift(sIx.data.get).map(_(k))
        } yield offset
      }

      def increaseSegmentOffset(s: Segment[F], delta: Offset): DbScript[F, Offset] = {
        DbScript.lift(s.offset.updateAndGet(_ + delta))
      }

      def updateSegmentIx(
        ix:     SegmentIx[F],
        key:    Key,
        offset: Offset
      ): DbScript[F, SegmentIx[F]] = {
        for {
          _ <- DbScript.lift(ix.data.update(_.updated(key, offset)))
        } yield ix
      }

      def updateTblIx(
        ix:  TblIx[F],
        key: Key,
        s:   Segment[F]
      ): DbScript[F, TblIx[F]] = {
        for {
          _ <- DbScript.lift(ix.data.update(_.updated(key, s)))
        } yield ix
      }
    }

    trait FileService[F[_]: Async] {

      def createDirIfNotExists(path: Path): DbScript[F, Path] = {
        DbScript.lift { Async[F].blocking(Files.createDirectories(path)) }
      }

      def createFile(path: Path): DbScript[F, Path] = {
        DbScript.lift { Async[F].blocking(Files.createFile(path)) }
      }

      def appendToFile(
        path:   Path,
        record: FileRecord
      ): DbScript[F, Unit] = {
        DbScript.lift {
          Async[F].blocking(Files.write(path, record, StandardOpenOption.CREATE, StandardOpenOption.APPEND))
        }
      }

      /*
        binary record format:
        [ record size | fld_1 size | fld_1 | fld_2 size | fld_2 | ... | fld_N size | fld_N ]
       */
      def readFileRecord(
        path:               Path,
        offset:             Offset,
        recordSizeCapacity: Int = 4
      ): DbScript[F, FileRecord] = {
        val stream = new java.io.FileInputStream(path.toFile)
        val ch = stream.getChannel
        val bSize = ByteBuffer.allocate(recordSizeCapacity)
        ch.position(offset)
        ch.read(bSize)
        val recordSize = bSize.getInt
        val bRecord = ByteBuffer.allocate(recordSize)
        ch.read(bRecord)
        DbScript.lift(bRecord.array().pure[F])
      }

    }

    trait BaseService[F[_]: Async] {

      def createIfNotExists(rootDir: String, baseName: BaseName): DbScript[F, Base[F]] = {
        for {
          env  <- ask[F, Env[F]]
          base <- DbScript.lift(Base.create(env.conf.rootDir, baseName))
          _    <- env.files.createDirIfNotExists(base.path)
          _    <- env.cache.addBase(base)
        } yield base
      }

      def get(baseName: BaseName): DbScript[F, Base[F]] = {
        for {
          env  <- ask[F, Env[F]]
          base <- env.cache.getBase(baseName)
        } yield base
      }

      def list(rootDir: String): DbScript[F, Seq[Base[F]]] = ???
    }

    trait TblService[F[_]: Async] {

      def createIfNotExists(base: Base[F], tblName: TblName): DbScript[F, Tbl[F]] = {
        for {
          env <- ask[F, Env[F]]
          tbl <- DbScript.lift(Tbl.create(base, tblName))
          _   <- env.files.createDirIfNotExists(tbl.path)
          _   <- env.cache.addTbl(base, tbl)
          _   <- env.tblIx.create(tbl)
          s   <- env.segment.create(tbl, 1)
          _   <- env.segmentIx.create(s)
        } yield tbl
      }

      def get(base: Base[F], tblName: TblName): DbScript[F, Tbl[F]] = {
        for {
          env <- ask[F, Env[F]]
          tbl <- env.cache.getTbl(base, tblName)
        } yield tbl
      }

      def appendToLastSegment(
        tbl:   Tbl[F],
        key:   Key,
        value: Value
      ): DbScript[F, Segment[F]] = {
        for {
          env <- ask[F, Env[F]]
          s   <- env.tbl.getOrAddLastSegment(tbl)
          _   <- env.segment.appendValue(s, key, value)
          tIx <- env.tbl.getOrAddTblIx(tbl)
          _   <- env.tblIx.update(tIx, key, s)
        } yield s
      }

      private def getOrAddLastSegment(tbl: Tbl[F]): DbScript[F, Segment[F]] = {
        for {
          env  <- ask[F, Env[F]]
          sOpt <- env.cache.getLastSegment(tbl)
          s <- sOpt match {
            case Some(s) => DbScript.lift(s.pure[F])
            case None =>
              for {
                s <- DbScript.lift(Segment.create(tbl))
                _ <- env.cache.addSegment(tbl, s)
              } yield s
          }
        } yield s
      }

      def findInSegments(
        tbl: Tbl[F],
        k:   Key
      ): DbScript[F, Option[Value]] = {
        for {
          env <- ask[F, Env[F]]
          tIx <- env.tbl.getOrAddTblIx(tbl)
          vOpt <- env.cache.findSegment(tIx, k) >>= {
            case Some(s) => env.segment.getValue(s, k).map(Some(_))
            case None    => DbScript.lift(None.pure[F])
          }
        } yield vOpt
      }

      private def getOrAddTblIx(tbl: Tbl[F]): DbScript[F, TblIx[F]] = {
        for {
          env   <- ask[F, Env[F]]
          ixOpt <- env.cache.getTblIx(tbl)
          ix <- ixOpt match {
            case Some(ix) => DbScript.lift(ix.pure[F])
            case None =>
              for {
                ix <- DbScript.lift(TblIx.create(tbl))
                _  <- env.cache.addTblIx(tbl, ix)
              } yield ix
          }
        } yield ix
      }

    }

    trait SegmentService[F[_]: Async] {

      def create(tbl: Tbl[F], num: SegmentNum): DbScript[F, Segment[F]] = {
        for {
          env <- ask[F, Env[F]]
          s   <- DbScript.lift(Segment.create(tbl))
          _   <- env.files.createFile(s.path)
          _   <- env.cache.addSegment(tbl, s)
        } yield s
      }

      def appendValue(
        s:     Segment[F],
        key:   Key,
        value: Value
      ): DbScript[F, Offset] = {
        for {
          env       <- ask[F, Env[F]]
          record    <- DbScript.lift(SegmentRecord.create(value))
          _         <- env.files.appendToFile(s.path, record)
          sIx       <- env.segment.getOrAddSegmentIx(s)
          offset    <- env.cache.getSegmentOffset(s)
          _         <- env.segmentIx.update(sIx, key, offset)
          newOffset <- env.cache.increaseSegmentOffset(s, record.size)
        } yield newOffset
      }

      def getOrAddSegmentIx(s: Segment[F]): DbScript[F, SegmentIx[F]] = {
        for {
          env   <- ask[F, Env[F]]
          ixOpt <- env.cache.getSegmentIx(s)
          ix <- ixOpt match {
            case Some(ix) => DbScript.lift(ix.pure[F])
            case None =>
              for {
                ix <- DbScript.lift(SegmentIx.create(s))
                _  <- env.cache.addSegmentIx(s, ix)
              } yield ix
          }
        } yield ix
      }

      def getValue(s: Segment[F], k: Key): DbScript[F, Value] = {
        for {
          env    <- ask[F, Env[F]]
          sIx    <- getOrAddSegmentIx(s)
          offset <- env.cache.getOffsetBySegmentIx(sIx, k)
          record <- env.files.readFileRecord(s.path, offset)
          v      <- DbScript.lift(SegmentRecord.getValue(record))
        } yield v
      }
    }

    trait TblIxService[F[_]: Async] {

      def create(tbl: Tbl[F]): DbScript[F, TblIx[F]] = {
        for {
          env <- ask[F, Env[F]]
          ix  <- DbScript.lift(TblIx.create(tbl))
          _   <- env.files.createFile(ix.path)
          _   <- env.cache.addTblIx(tbl, ix)
        } yield ix
      }

      def read(tbl: Tbl[F]): DbScript[F, TblIx[F]] = ???

      def write(ix: TblIx[F]): DbScript[F, TblIx[F]] = ???

      def update(
        ix:  TblIx[F],
        key: Key,
        s:   Segment[F]
      ): DbScript[F, TblIx[F]] = {
        for {
          env    <- ask[F, Env[F]]
          record <- DbScript.lift(TblIxRecord.create(key, s))
          _      <- env.files.appendToFile(ix.path, record)
          _      <- env.cache.updateTblIx(ix, key, s)
        } yield ix
      }

    }

    trait SegmentIxService[F[_]: Async] {

      def create(s: Segment[F]): DbScript[F, SegmentIx[F]] = {
        for {
          env <- ask[F, Env[F]]
          ix  <- DbScript.lift(SegmentIx.create(s))
          _   <- env.files.createFile(ix.path)
          _   <- env.cache.addSegmentIx(s, ix)
        } yield ix
      }

      def update(
        ix:     SegmentIx[F],
        key:    Key,
        offset: Offset
      ): DbScript[F, SegmentIx[F]] = {
        for {
          env    <- ask[F, Env[F]]
          record <- DbScript.lift(SegmentIxRecord.create(key, offset))
          _      <- env.files.appendToFile(ix.path, record)
          _      <- env.cache.updateSegmentIx(ix, key, offset)
        } yield ix
      }

      def read(tbl: Tbl[F], num: SegmentNum): DbScript[F, SegmentIx[F]] = ???

      def write(ix: SegmentIx[F]): DbScript[F, SegmentIx[F]] = ???
    }

    trait LibScenarios[F[_]: Async] {
      given env: Env[F]

      def createBaseIfNotExists(baseName: String): F[Base[F]] =
        DbScript.run {
          for {
            env  <- ask[F, Env[F]]
            base <- env.base.createIfNotExists(env.conf.rootDir, baseName)
          } yield base
        }

      def createTableIfNotExists(baseName: String, tblName: String): F[Tbl[F]] =
        DbScript.run {
          for {
            env  <- ask[F, Env[F]]
            base <- env.base.get(baseName)
            tbl  <- env.tbl.createIfNotExists(base, tblName)
          } yield tbl
        }

      def get(
        baseName: String,
        tblName:  String,
        key:      String
      ): F[Option[String]] = {
        DbScript.run {
          for {
            env  <- ask[F, Env[F]]
            base <- env.base.get(baseName)
            tbl  <- env.tbl.get(base, tblName)
            vOpt <- env.tbl.findInSegments(tbl, key)
          } yield vOpt
        }
      }

      def set(
        baseName: String,
        tblName:  String,
        key:      String,
        value:    String
      ): F[Unit] = {
        DbScript.run {
          for {
            env  <- ask[F, Env[F]]
            base <- env.base.get(baseName)
            tbl  <- env.tbl.get(base, tblName)
            _    <- env.tbl.appendToLastSegment(tbl, key, value)
          } yield ()
        }
      }
    }

    final case class Base[F[_]: Async](
      name:   BaseName,
      path:   Path,
      tables: Ref[F, BaseTables[F]])

    final case class TblIx[F[_]: Async](
      name: TblIxName,
      path: Path,
      data: Ref[F, TblIxData[F]])

    final case class SegmentIx[F[_]: Async](
      name: SegmentIxName,
      path: Path,
      data: Ref[F, SegmentIxData])

    final case class Tbl[F[_]: Async](
      name:        TblName,
      path:        Path,
      lastSegment: Ref[F, Option[Segment[F]]],
      ix:          Ref[F, Option[TblIx[F]]])

    final case class Segment[F[_]: Async](
      num:        SegmentNum,
      name:       SegmentName,
      path:       Path,
      ix:         Ref[F, Option[SegmentIx[F]]],
      offset:     Ref[F, Offset],
      isReadOnly: Boolean = false)

    final case class State[F[_]: Async](registryRef: Ref[F, BaseRegistry[F]])

    object Tbl {

      def create[F[_]: Async](base: Base[F], name: TblName): F[Tbl[F]] = {
        val path = Paths.get(f"${base.path.toAbsolutePath}/$name")
        for {
          lastSegment <- Async[F].ref(Option.empty[Segment[F]])
          ix          <- Async[F].ref(Option.empty[TblIx[F]])
        } yield Tbl(name, path, lastSegment, ix)
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

    object TblIx {

      def create[F[_]: Async](tbl: Tbl[F]): F[TblIx[F]] = {
        val name = "table.ix"
        val path = Paths.get(f"${tbl.path.toAbsolutePath}/$name")
        for {
          data <- Async[F].ref(Map.empty[Key, Segment[F]])
        } yield TblIx(name, path, data)
      }
    }

    object TblIxRecord {

      def create[F[_]: Async](key: Key, s: Segment[F]): F[FileRecord] = {
        val keyBytes = key.getBytes("UTF-8")
        val keySize = keyBytes.length
        val sNameBytes = s.name.getBytes
        val sNameSize = sNameBytes.length
        (ByteBuffer.allocate(4).putInt(keySize).array()
          ++ keyBytes
          ++ ByteBuffer.allocate(4).putInt(sNameSize).array()
          ++ sNameBytes).pure[F]
      }
    }

    object Segment {

      def create[F[_]: Async](
        tbl: Tbl[F],
        num: SegmentNum = 1
      ): F[Segment[F]] = {
        val name = f"segment_$num.seg"
        val path = Paths.get(f"${tbl.path.toAbsolutePath}/$name")
        for {
          ix     <- Async[F].ref(Option.empty[SegmentIx[F]])
          offset <- Async[F].ref(0L)
        } yield Segment(num, name, path, ix, offset)
      }
    }

    object SegmentRecord {
      private val recordSizeCapacity = 4

      def create[F[_]: Async](value: Value): F[FileRecord] = {
        val valueBytes = value.getBytes("UTF-8")
        val valueSize = valueBytes.length
        val valueSizeBytes = ByteBuffer.allocate(recordSizeCapacity).putInt(valueSize).array()
        val recordSize = recordSizeCapacity + valueSize
        val recordSizeBytes = valueSizeBytes

        (recordSizeBytes ++ valueBytes).pure[F]
      }

      def getValue[F[_]: Async](r: FileRecord): F[Value] = {
        val value = new String(r, "UTF-8")
        value.pure[F]
      }
    }

    object SegmentIx {

      def create[F[_]: Async](s: Segment[F]): F[SegmentIx[F]] = {
        val name = f"${s.name}.ix"
        val path = Paths.get(f"${s.path.toAbsolutePath}/$name")
        for {
          data <- Async[F].ref(Map.empty[Key, Offset])
        } yield SegmentIx(name, path, data)
      }
    }

    object SegmentIxRecord {
      private val keySizeCapacity:    Int = 4
      private val recordSizeCapacity: Int = 4
      private val offsetCapacity:     Int = 8

      def create[F[_]: Async](key: Key, offset: Offset): F[FileRecord] = {
        val keyBytes = key.getBytes("UTF-8")
        val keySize = keyBytes.length
        val keySizeBytes = ByteBuffer.allocate(keySizeCapacity).putInt(keySize).array()
        val offsetBytes = ByteBuffer.allocate(offsetCapacity).putLong(offset).array()
        val recordSize = keySizeCapacity + keySize + offsetCapacity
        val recordSizeBytes = ByteBuffer
          .allocate(recordSizeCapacity)
          .putInt(recordSize)
          .array()
        (recordSizeBytes
          ++ keySizeBytes
          ++ keyBytes
          ++ offsetBytes).pure[F]
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
}
