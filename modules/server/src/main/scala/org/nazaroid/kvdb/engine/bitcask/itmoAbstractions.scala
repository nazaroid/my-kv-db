package org.nazaroid.kvdb.engine.bitcask

import org.nazaroid.kvdb.algebra.DatabaseException

import java.io.IOException
import java.nio.file.Path


@FunctionalInterface trait DatabaseFactory {
  /**
   * Создает базу данных с указанным именем, если такая база еще не существует.
   *
   * @param dbName имя базы данных
   * @param dbRoot путь до директории, в которой будет создана база данных
   * @return объект созданной бд
   * @throws DatabaseException если база данных с данным именем уже существует или если произошла ошибка ввода-вывода
   */
  @throws[DatabaseException]
  def createNonExistent(dbName: String, dbRoot: Path): Nothing
}

trait Database {
  /**
   * Возвращает имя базы данных.
   *
   * @return имя базы данных
   */
  def getName: String

  /**
   * Создает таблицу с указанным именем, если это имя еще не занято.
   *
   * @param tableName имя таблицы
   * @throws DatabaseException если таблица с данным именем уже существует или если произошла ошибка ввода-вывода
   */
  @throws[DatabaseException]
  def createTableIfNotExists(tableName: String): Unit

  /**
   * Записывает значение в указанную таблицу по переданному ключу.
   *
   * @param tableName   таблица, в которую нужно записать значение
   * @param objectKey   ключ, по которому нужно записать значение
   * @param objectValue значение, которое нужно записать
   * @throws DatabaseException если указанная таблица не была найдена или если произошла ошибка ввода-вывода
   */
  @throws[DatabaseException]
  def write(tableName: String, objectKey: String, objectValue: Array[Byte]): Unit

  /**
   * Считывает значение из указанной таблицы по заданному ключу.
   *
   * @param tableName таблица, из которой нужно считать значение
   * @param objectKey ключ, по которому нужно получить значение
   * @return значение, которое находится по ключу
   * @throws DatabaseException если не была найдена указанная таблица, или произошла ошибка ввода-вывода
   */
  @throws[DatabaseException]
  def read(tableName: String, objectKey: String): Option[Array[Byte]]

  @throws[DatabaseException]
  def delete(tableName: String, objectKey: String): Unit
}

trait DatabaseCache {
  def get(key: String): Array[Byte]

  def set(key: String, value: Array[Byte]): Unit

  def delete(key: String): Unit
}
/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
trait Table {
  /**
   * Возвращает имя таблицы.
   *
   * @return имя таблицы
   */
  def getName: String

  /**
   * Записывает в таблицу переданное значение по указанному ключу.
   *
   * @param objectKey   ключ, по которому нужно записать значение
   * @param objectValue значение, которое нужно записать
   * @throws DatabaseException если произошла ошибка ввода-вывода
   */
  @throws[DatabaseException]
  def write(objectKey: String, objectValue: Array[Byte]): Unit

  /**
   * Считывает значение из таблицы по заданному ключу.
   *
   * @param objectKey ключ, по которому нужно получить значение
   * @return значение, которое находится по ключу
   * @throws DatabaseException если произошла ошибка ввода-вывода
   */
  @throws[DatabaseException]
  def read(objectKey: String): Option[Array[Byte]]

  @throws[DatabaseException]
  def delete(objectKey: String): Unit
}

trait Segment {
  /**
   * Возвращает имя сегмента.
   *
   * @return имя сегмента
   */
  def getName: String

  /**
   * Записывает значение по указанному ключу в сегмент.
   *
   * @param objectKey   ключ, по которому нужно записать значение
   * @param objectValue значение, которое нужно записать
   * @return {@code true} - если значение записалось, {@code false} - если нет
   * @throws IOException если произошла ошибка ввода-вывода.
   */
  @throws[IOException]
  def write(objectKey: String, objectValue: Array[Byte]): Boolean

  /**
   * Считывает значение из сегмента по переданному ключу.
   *
   * @param objectKey ключ, по которому нужно получить значение
   * @return значение, которое находится по ключу
   * @throws IOException если произошла ошибка ввода-вывода
   */
  @throws[IOException]
  def read(objectKey: String): Option[Array[Byte]]

  /**
   * Возвращает {@code true} - если данный сегмент открыт только на чтение, {@code false} - если данный сегмент открыт на чтение и запись.
   *
   * @return {@code true} - если данный сегмент открыт только на чтение, {@code false} - если данный сегмент открыт на чтение и запись
   */
  def isReadOnly: Boolean

  @throws[IOException]
  def delete(objectKey: String): Boolean
}


/**
 * Представляет собой единицу хранения в БД
 */
trait DatabaseRecord {
  /**
   * Возвращает ключ
   */
  def getKey: Array[Byte]

  /**
   * Возвращает значение
   */
  def getValue: Array[Byte]

  /**
   * Возвращает размер хранимой записи в базе данных. Используется для определения offset (сдвига)
   */
  def size: Long

  /**
   * Индикатор, есть ли значение
   */
  def isValuePresented: Boolean
}

trait WritableDatabaseRecord extends DatabaseRecord {
  /**
   * Возвращает размер ключа в байтах
   */
  def getKeySize: Int

  /**
   * Возвращает размер значения в байтах. -1, если значение отсутствует
   */
  def getValueSize: Int
}



trait KvsIndex[K, V] {
  /**
   * Оповещает индекс об обновлении значения по определенному ключу.
   *
   * @param key   ключ, который обновился
   * @param value новое значение
   */
  def onIndexedEntityUpdated(key: K, value: V): Unit

  /**
   * Ищет значение в индексе по указанному ключу.
   *
   * @param key ключ, по которому нужно провести поиск значение
   * @return {@code Optional<V>}
   */
  def searchForKey(key: K): Option[V]
}

trait SegmentOffsetInfo {
  def getOffset: Long
}
