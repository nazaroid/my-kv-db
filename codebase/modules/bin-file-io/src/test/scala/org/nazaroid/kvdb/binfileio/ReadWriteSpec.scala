package org.nazaroid.kvdb.binfileio

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Async, IO}
import fs2.Stream
import org.scalatest.FutureOutcome
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jFactory

import java.nio.file.{Files, Paths}
import scala.concurrent.duration.DurationInt
import scala.reflect.io.Directory

final class ReadWriteSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    Files.createDirectories(testDir)
    val outcome = super.withFixture(test)
    outcome.onCompletedThen { _ =>
      val dir = new Directory(testDir.toFile)
      if (dir.exists) {
        dir.deleteRecursively()
      }
    }
  }

  private val testDir = Paths.get("./testFolder")

  private given logger: Logger[IO] = Slf4jFactory.create[IO].getLogger
  
  "should write and read same data" in {

    val path = f"$testDir/segment_1.ix"
    val schema = List(
      FieldDef("keySize", FieldType.Int32),
      FieldDef("key", FieldType.StringUtf8(sizeFromField = "keySize")),
      FieldDef("offset", FieldType.Int64)
    )
    val writtenRows: List[Row] = List(
      Map("keySize" -> 4, "key" -> "user", "offset"  -> 1024L),
      Map("keySize" -> 5, "key" -> "admin", "offset" -> 2048L)
    )

    val inputWriteTasks = writtenRows.map(r => WriteTask[IO]("key", path, schema = schema, row = r, None))

    for {
      _        <- BinFileIO.writeAll[IO](Stream.emits(inputWriteTasks), 2).compile.drain
      _        <- Async[IO].sleep(100 millis)
      readRows <- BinFileIO.readAll[IO](path, schema).map(_._2).compile.toList
    } yield assert(readRows == writtenRows)
  }
}
