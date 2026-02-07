package org.nazaroid.kvdb.binfileio

import cats.effect.*
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.concurrent.Channel
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration.DurationInt

final class DiskStorageSpec extends AnyFlatSpecLike {
  import DiskStorageSpec.*

  it should "write and read same data" in {
    (for {
      ix     <- Async[IO].ref(Map[String, DiskStorageValue]())
      buffer <- Channel.bounded[IO, WriteTask[IO]](2)
      s = DiskStorage[IO](ix, buffer, path, schema, keyField)
      _  <- Stream.emits(writtenRows).evalMap(r => s.write(r(keyField).toString, r)).compile.drain
      _  <- Async[IO].sleep(100 millis)
      r1 <- s.read("user")
      r2 <- s.read("admin")
      readRows = List(r1, r2).flatten
    } yield assert(writtenRows == readRows)).unsafeRunSync()
  }

  it should "be able to recover index" in {
    (for {
      ix     <- Async[IO].ref(Map[String, DiskStorageValue]())
      buffer <- Channel.bounded[IO, WriteTask[IO]](2)
      s = DiskStorage[IO](ix, buffer, path, schema, keyField)
      _                 <- Stream.emits(writtenRows).evalMap(r => s.write(r(keyField).toString, r)).compile.drain
      _                 <- Async[IO].sleep(100 millis)
      dataBeforeRecover <- ix.get
      _                 <- s.recoverIndex()
      dataAfterRecover  <- ix.get
      r1                <- s.read("user")
      r2                <- s.read("admin")
      readRows = List(r1, r2).flatten
    } yield assert(writtenRows == readRows && dataBeforeRecover == dataAfterRecover)).unsafeRunSync()
  }
}

object DiskStorageSpec {
  val path = "./segment_1.ix"

  val schema: List[FieldDef] = List(
    FieldDef("keySize", FieldType.Int32),
    FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
    FieldDef("offset", FieldType.Int64)
  )
  val keyField = "key"

  val writtenRows: List[Row] = List(
    Map("keySize" -> 4, "key" -> "user", "offset"  -> 1024L),
    Map("keySize" -> 5, "key" -> "admin", "offset" -> 2048L)
  )
}
