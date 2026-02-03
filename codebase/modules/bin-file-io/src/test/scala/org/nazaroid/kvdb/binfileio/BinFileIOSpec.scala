package org.nazaroid.kvdb.binfileio

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import org.scalatest.flatspec.AnyFlatSpecLike

final class BinFileIOSpec extends AnyFlatSpecLike {

  it should "write and read same data" in {
    val schema = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("offset", FieldType.Int64)
    )
    val writtenRows: List[Row] = List(
      Map("keySize" -> 4, "key" -> "user", "offset"  -> 1024L),
      Map("keySize" -> 5, "key" -> "admin", "offset" -> 2048L)
    )

    (for {
      _        <- BinFileIO.write[IO]("./segment_1.ix", schema, Stream.emits(writtenRows)).compile.drain
      readRows <- BinFileIO.read[IO]("./segment_1.ix", schema).compile.toList

    } yield assert(writtenRows == readRows)).unsafeRunSync()
  }
}
