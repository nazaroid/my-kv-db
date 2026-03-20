package org.nazaroid.kvdb.engine.examples

import cats.effect.{Async, Resource}
import cats.implicits.given
import org.nazaroid.kvdb.algebra.{Engine, DatabaseStats}
import org.typelevel.log4cats.Logger
import io.circe.syntax._
import fs2.io.file.{Files, Path}

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable

/**
 * Example of single-file engine with heterogeneous statistics
 */
class SingleFileEngine[F[_]: Async: Logger: Files](filePath: String) extends Engine[F] {
  
  override def createDbIfNotExists(name: String): F[Unit] = {
    // In single-file engine, all data goes to one file
    Async[F].unit
  }
  
  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    // In single-file engine, all data goes to one file
    Async[F].unit
  }
  
  override def get(baseName: String, tblName: String, key: String): F[Option[String]] = {
    for {
      exists <- Files[F].exists(Path(filePath))
      result <- if (exists) {
        // Read from file (simplified)
        Async[F].delay {
          val fis = new FileInputStream(filePath)
          val ois = new ObjectInputStream(fis)
          try {
            val data = ois.readObject().asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]
            data.get(baseName).flatMap(_.get(tblName)).flatMap(_.get(key))
          } finally {
            ois.close()
            fis.close()
          }
        }
      } else {
        Async[F].pure(None)
      }
    } yield result
  }
  
  override def set(baseName: String, tblName: String, key: String, value: String): F[Unit] = {
    for {
      exists <- Files[F].exists(Path(filePath))
      currentData <- if (exists) {
        Async[F].delay {
          val fis = new FileInputStream(filePath)
          val ois = new ObjectInputStream(fis)
          try {
            ois.readObject().asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]
          } finally {
            ois.close()
            fis.close()
          }
        }
      } else {
        Async[F].pure(mutable.Map.empty[String, mutable.Map[String, String]])
      }
      
      _ <- Async[F].delay {
        currentData.getOrElseUpdate(baseName, mutable.Map.empty)
          .getOrElseUpdate(tblName, mutable.Map.empty)
          .put(key, value)
        
        val fos = new FileOutputStream(filePath)
        val oos = new ObjectOutputStream(fos)
        try {
          oos.writeObject(currentData)
        } finally {
          oos.close()
          fos.close()
        }
      }
    } yield ()
  }
  
  override def delete(baseName: String, tblName: String, key: String): F[Unit] = {
    for {
      exists <- Files[F].exists(Path(filePath))
      _ <- if (exists) {
        Async[F].delay {
          val fis = new FileInputStream(filePath)
          val ois = new ObjectInputStream(fis)
          try {
            val data = ois.readObject().asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]
            data.get(baseName).flatMap(_.get(tblName)).foreach(_.remove(key))
            
            val fos = new FileOutputStream(filePath)
            val oos = new ObjectOutputStream(fos)
            try {
              oos.writeObject(data)
            } finally {
              oos.close()
              fos.close()
            }
          } finally {
            ois.close()
            fis.close()
          }
        }
      } else {
        Async[F].unit
      }
    } yield ()
  }
  
  override def getStats: F[DatabaseStats] = {
    for {
      exists <- Files[F].exists(Path(filePath))
      stats <- if (exists) {
        for {
          fileSize <- Files[F].size(Path(filePath))
          data <- Async[F].delay {
            val fis = new FileInputStream(filePath)
            val ois = new ObjectInputStream(fis)
            try {
              ois.readObject().asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]
            } finally {
              ois.close()
              fis.close()
            }
          }
          
          totalTables = data.size
          totalEntries = data.values.map(_.size).sum
          activeEntries = totalEntries
          deletedEntries = 0
          
        } yield DatabaseStats(
          totalTables = totalTables,
          totalEntries = totalEntries,
          activeEntries = activeEntries,
          deletedEntries = deletedEntries,
          totalDataSize = fileSize,
          details = Map(
            "engine_type" -> "single_file".asJson,
            "file_path" -> filePath.asJson,
            "file_size_bytes" -> fileSize.asJson,
            "file_size_mb" -> (fileSize / 1024 / 1024).asJson,
            "serialization_format" -> "java_serialization".asJson,
            "compression" -> false.asJson,
            "backup_count" -> 0.asJson,
            "last_modified" -> System.currentTimeMillis().asJson
          )
        )
      } else {
        Async[F].pure(DatabaseStats(
          totalTables = 0,
          totalEntries = 0,
          activeEntries = 0,
          deletedEntries = 0,
          totalDataSize = 0,
          details = Map(
            "engine_type" -> "single_file".asJson,
            "file_path" -> filePath.asJson,
            "file_exists" -> false.asJson
          )
        ))
      }
    } yield stats
  }
}

object SingleFileEngine {
  def create[F[_]: Async: Logger: Files](filePath: String): Resource[F, Engine[F]] = {
    Resource.pure(new SingleFileEngine[F](filePath))
  }
}
