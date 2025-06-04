package org.nazaroid.kvdb.engine.bitcask

import cats.effect.Async
import org.nazaroid.kvdb.algebra.{Database, DbEngine, Table}

final class BitcaskDbEngine[F[_]: Async] extends DbEngine[F] {

  override def createDbIfNotExists(name: String): F[Database[F]] = {
    /* TODO:
      создать общий конвейер для всех команд
      * передаем команду в stream с pipe-ом для колбека и ждем ответ
      * при createDbIfNotExists:
       - создать папку для  бд
     */
    ???
  }
}

object BitcaskDbEngine {

  private class BitcaskDatabase[F[_]: Async] extends Database[F] {

    override def createTableIfNotExists(name: String): F[Table[F]] = {
      // TODO:
      // таблицы и базы хранить в памяти, загружать при старте, потом в рилтайме модифицировать список
      // для новой:
      // - создать с таблицей внутри папки БД
      // - инициализировать новый пустой сегмент
      //   -- создать файлы индексов (таблицы + сегмента) и сегмента
      // для существующей:
      // - ничего не делать, вернуть объект
      ???
    }
  }

  private class BitcaskTable[F[_]: Async] extends Table[F] {
    override def get(key: String): F[String] = ???

    override def set(key: String, value: String): F[Unit] = {
      ???
    }
  }
}
