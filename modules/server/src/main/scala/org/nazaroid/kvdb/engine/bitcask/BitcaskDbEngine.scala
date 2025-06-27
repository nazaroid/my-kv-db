package org.nazaroid.kvdb.engine.bitcask

import cats.effect.Async
import cats.implicits.*
import org.nazaroid.kvdb.BitcaskConf
import org.nazaroid.kvdb.algebra.{Database, DbEngine, Table}
import org.nazaroid.kvdb.engine.bitcask.BitcaskDbEngine.*
import org.nazaroid.kvdb.engine.bitcask.BitcaskLib.algebra.*

final class BitcaskDbEngine[F[_]: Async](conf: BitcaskConf) extends DbEngine[F] {

  override def createDbIfNotExists(name: String): F[Database[F]] = {
    /* TODO:
      создать общий конвейер для всех команд
     * передаем команду в stream с pipe-ом для колбека и ждем ответ
     * при createDbIfNotExists:
       - создать папку для  бд
     */
    lib.createBaseIfNotExists(name).map(BitcaskDatabase[F])
  }

  private given lib: LibScenarios[F] = BitcaskLib(conf)
}

object BitcaskDbEngine {

  case class BitcaskDatabase[F[_]: Async](base: Base)(using lib: LibScenarios[F])
      extends Database[F] {

    override def createTableIfNotExists(name: String): F[Table[F]] = {
      // TODO:
      // таблицы и базы хранить в памяти, загружать при старте, потом в рилтайме модифицировать список
      // для новой:
      // - создать папку с таблицей внутри папки БД
      // - инициализировать новый пустой сегмент
      //   -- создать файлы индексов (таблицы + сегмента) и сегмента
      // для существующей:
      // - ничего не делать, вернуть объект

      lib.createTableIfNotExists(base, name).map(BitcaskTable[F])
    }
  }

  case class BitcaskTable[F[_]: Async](tbl: Tbl)(using lib: LibScenarios[F])
      extends Table[F] {

    override def get(key: String): F[String] = ???

    override def set(key: String, value: String): F[Unit] = {
      ???
    }
  }

}
