package org.nazaroid.kvdb.engine.bitcask

import cats.effect.Async
import cats.implicits.*
import org.nazaroid.kvdb.algebra.DbEngine
import org.nazaroid.kvdb.bitcask.lib.BitcaskConf
import org.nazaroid.kvdb.bitcask.lib.algebra.*

final class BitcaskDbEngine[F[_]: Async](conf: BitcaskConf, lib: LibScenarios[F]) extends DbEngine[F] {

  override def createDbIfNotExists(name: String): F[Unit] = {
    /* TODO:
      создать общий конвейер для всех команд
     * передаем команду в stream с pipe-ом для колбека и ждем ответ
     * при createDbIfNotExists:
       - создать папку для  бд
     */
    lib.createBaseIfNotExists(name) >> ().pure[F]
  }

  override def createTableIfNotExists(baseName: String, tblName: String): F[Unit] = {
    // TODO:
    // таблицы и базы хранить в памяти, загружать при старте, потом в рилтайме модифицировать список
    // для новой:
    // - создать папку с таблицей внутри папки БД
    // - инициализировать новый пустой сегмент
    //   -- создать файлы индексов (таблицы + сегмента) и сегмента
    // для существующей:
    // - ничего не делать, вернуть объект

    lib.createTableIfNotExists(baseName, tblName) >> ().pure[F]
  }

  override def get(
    baseName: String,
    tblName:  String,
    key:      String
  ): F[Option[String]] = lib.get(baseName, tblName, key)

  override def set(
    baseName: String,
    tblName:  String,
    key:      String,
    value:    String
  ): F[Unit] = {
    lib.set(baseName, tblName, key, value)
  }

}
