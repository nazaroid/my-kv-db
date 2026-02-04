package org.nazaroid.kvdb.binfileio

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO}
import fs2.Stream
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration.DurationInt

final class BinFileIOSpec extends AnyFlatSpecLike {

  it should "write and read same data" in {
    val path = "./segment_1.ix"
    val schema = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("offset", FieldType.Int64)
    )
    val writtenRows: List[Row] = List(
      Map("keySize" -> 4, "key" -> "user", "offset"  -> 1024L),
      Map("keySize" -> 5, "key" -> "admin", "offset" -> 2048L)
    )

    val inputWriteTasks = writtenRows.map(r => WriteTask(path, schema = schema, row = r))

    (for {
      _        <- BinFileIO.write[IO](Stream.emits(inputWriteTasks), 2).compile.drain
      _        <- Async[IO].sleep(100 millis)
      readRows <- BinFileIO.read[IO](path, schema).compile.toList

    } yield assert(writtenRows == readRows)).unsafeRunSync()
  }
}
