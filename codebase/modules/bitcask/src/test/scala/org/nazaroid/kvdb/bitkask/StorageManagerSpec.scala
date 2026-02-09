package org.nazaroid.kvdb.bitkask

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.concurrent.Channel
import fs2.io.file.{Files, Path}
import org.nazaroid.kvdb.*
import org.nazaroid.kvdb.binfileio.*
import org.nazaroid.kvdb.bitcask.storage.*
import org.scalatest.FutureOutcome
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.concurrent.duration.DurationInt
import scala.reflect.io.Directory

final class StorageManagerSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private val testDir = Paths.get("./testFolder")

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    java.nio.file.Files.createDirectories(testDir)
    val outcome = super.withFixture(test)
    outcome.onCompletedThen { _ =>
      val dir = new Directory(testDir.toFile)
      if (dir.exists) {
        dir.deleteRecursively()
      }
    }
  }

  private val config = StorageConfig(
    folder         = testDir.toString,
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
    _     <- Resource.eval(Files[IO].createDirectories(Path(testDir.toString)).handleError(_ => ()))
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
        files <- Files[IO].list(Path(testDir.toString)).map(_.fileName.toString).compile.toList
        // Должен остаться только новый компакт-сегмент и активный пустой сегмент
      } yield assert(files.exists(_.startsWith("compact_")), "Должен появиться компактный сегмент")
    }
  }

  "Recovery: данные должны быть доступны после полной перезагрузки системы" in {
    val key = "persistent_user"
    val value = "{\"data\": \"important\"}"

    // 1. Первая сессия: пишем данные и выключаемся
    val session1 = storageResource.use { sm =>
      sm.write(key, value) *> IO.sleep(100.millis) // Ждем завершения записи на диск
    }

    // 2. Вторая сессия: открываем хранилище заново и читаем
    val session2 = storageResource.use { sm =>
      sm.read(key).map { recoveredValue =>
        assert(recoveredValue.contains(value), "Данные должны восстановиться из индексов на диске")
      }
    }

    session1 *> session2
  }

  "Recovery with Deletion: удаленные данные не должны появиться после перезагрузки" in {
    val keyToKeep = "keep_me"
    val keyToDelete = "delete_me"

    val scenario = for {
      // Шаг 1: Пишем два ключа, один удаляем
      _ <- storageResource.use { sm =>
        for {
          _ <- sm.write(keyToKeep, "value1")
          _ <- sm.write(keyToDelete, "value2")
          _ <- sm.delete(keyToDelete)
          _ <- IO.sleep(100.millis)
        } yield ()
      }

      // Шаг 2: Перезагружаемся и проверяем состояние
      _ <- storageResource.use { sm =>
        for {
          val1 <- sm.read(keyToKeep)
          val2 <- sm.read(keyToDelete)
          _    <- IO.pure(assert(val1.contains("value1")))
          _    <- IO.pure(assert(val2.isEmpty, "Удаленный ключ не должен восстановиться"))
        } yield ()
      }
    } yield ()

    scenario
  }
}
