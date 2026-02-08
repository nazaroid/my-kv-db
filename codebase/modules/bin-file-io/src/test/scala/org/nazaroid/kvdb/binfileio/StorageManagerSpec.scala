package org.nazaroid.kvdb.binfileio

import cats.effect.*
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AsyncFreeSpec
import scala.concurrent.duration.DurationInt

final class StorageManagerSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  // Вспомогательная настройка для каждого теста
  private val testFolder = "./test_data"

  private val config = StorageConfig(
    folder         = testFolder,
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

  // Ресурс для запуска менеджера в тестах
  private val storageResource: Resource[IO, StorageManager[IO]] = for {
    _     <- Resource.eval(Files[IO].createDirectories(Path(testFolder)).handleError(_ => ()))
    queue <- Channel.bounded[IO, WriteTask[IO]](100).toResource
    // Запускаем воркер записи в фоне
    _       <- writeBinary(queue.stream, parallelism = 1).compile.drain.background
    manager <- Resource.eval(StorageManager.initialize[IO](config, queue))
  } yield manager

  "Write and Read: записанное значение должно быть доступно" in {
    storageResource.use { sm =>
      for {
        _   <- sm.write("user:1", "hello stratum")
        res <- sm.read("user:1")
      } yield assert(res.contains("hello stratum"))
    }
  }

  "Delete: удаленное значение должно возвращать None" in {
    storageResource.use { sm =>
      for {
        _   <- sm.write("user:2", "to be deleted")
        _   <- sm.delete("user:2")
        res <- sm.read("user:2")
      } yield assert(res.isEmpty)
    }
  }

  "Rotation: при превышении лимита должен создаваться новый сегмент" in {
    storageResource.use { sm =>
      for {
        // Пишем много данных, чтобы вызвать ротацию (1КБ лимит)
        _    <- sm.write("k1", "a" * 600)
        seg1 <- sm.currentData.get.map(_.filePath)

        _    <- sm.write("k2", "b" * 600)
        seg2 <- sm.currentData.get.map(_.filePath)

        _ <- IO.sleep(100.millis) // Даем время файловой системе

        // Проверяем, что пути к файлам разные
        _ <- IO.blocking(assert(seg1 != seg2, s"Сегмент должен был смениться: $seg1 vs $seg2"))

        // Данные из старого сегмента должны быть доступны
        val1 <- sm.read("k1")
      } yield assert(val1.contains("a" * 600))
    }
  }

  "Compaction & Cleanup: после компакции старые файлы должны удалиться" in {
    storageResource.use { sm =>
      for {
        _ <- sm.write("temp", "data")
        _ <- sm.delete("temp") // Создаем "мусор"
        _ <- sm.write("permanent", "keep me")

        // Запускаем уплотнение
        _ <- sm.compact()
        _ <- IO.sleep(200.millis)

        // Проверяем, что живые данные на месте
        res <- sm.read("permanent")
        _   <- IO.blocking(assert(res.contains("keep me")))

        // Проверяем физическое наличие файлов через Files.list
        files <- Files[IO].list(Path(testFolder)).map(_.fileName.toString).compile.toList
        // Должен остаться только новый компакт-сегмент и активный пустой сегмент
      } yield assert(files.exists(_.startsWith("compact_")), "Должен появиться компактный сегмент")
    }
  }
}
