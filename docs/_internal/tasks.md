# my-kv-db

## refs

https://drive.google.com/drive/u/1/folders/1BpJo8j4xkWuLI-9Zls8dZNmtWIdH_zwQ
https://docs.google.com/document/d/1sww6oOqIgJuGDw4UU746usO1lyFgargHtrjEj3av__k/edit?tab=t.0
https://app.diagrams.net/?libs=general;citrix#

https://www.youtube.com/watch?v=NITGB4SH-j8&t=335s
https://github.com/itmo-java-basics-2021/lab-base-code/tree/master/src/main/java/com/itmo/java

crc - Циклический избыточный код (англ. Cyclic redundancy check[нет в источнике], CRC[1]) — алгоритм нахождения
контрольной суммы, предназначенный для проверки целостности данных[2].
https://ru.wikipedia.org/wiki/%D0%A6%D0%B8%D0%BA%D0%BB%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8%D0%B9_%D0%B8%D0%B7%D0%B1%D1%8B%D1%82%D0%BE%D1%87%D0%BD%D1%8B%D0%B9_%D0%BA%D0%BE%D0%B4
в bitcast считается для каждой строки (см bitcask-intro.pdf на google диске)

## gpt prompt
```
  В данном проекте я разрабатываю kv-базу даных с движком bitcask на Scala 3 + Cats. Структура проекта уже создана, все упоминаемые в задаче файлы уже существуют.
  Чтобы разобраться в структуре проекта изучи документацию, начиная с README.md и документы в папке docs .
  Код kv-базы находится в папке codebase. А в папке playground тестовая среда для обкатки.
```

## TODO

--- 
### Цель: выложить в open-source

---

- (TODO) проработать последнее замечание CODE_REVIEW
  - ввести конфиг `durability: Relaxed | FsyncPerSegment | FsyncPerBatch`
  - удалить потом файл [code_review.md](code_review.md)

- (TODO) подготовить для OpenSource
  - README.md
    + дополнить ссылкой на docs
    + логотип
  - провести Code Review и провести рефакторинги
  + придумать другое имя, переименовть и выложить в github

---

* (TODO) сейчас БД не держит нагрузку
  - {"@timestamp":"2026-07-18T20:12:36.428+00:00","message":"Error servicing request: POST /mydb/mytable/key_5429 from 172.24.0.5","logger_name":"org.nazaroid.kvdb.srv.DbInstance","thread_name":"io-compute-blocker-323","level":"ERROR","stack_trace":"java.nio.file.NoSuchFileException:
  
* (TODO) добавить типы
  Blob / Byte Array - базовый тип для хранения
  Bitcask Engine: возвращает Byte Array
  Serialization/Codec Layer: работает поверх engine и производит преобразования
  String (Строка): Основной тип для ключей. Позволяет организовывать пространство имен (например, user:123:profile) и выполнять поиск по маскам.
  Integer (Целое число): Необходим для реализации атомарных счетчиков и инкрементальных операций без полной перезаписи значения.
  Boolean (Логический тип): Часто используется для флагов состояния, так как занимает минимум места.
* 
- (TODO) MVP: попробовать переделать на DSL + Free (можно только AppL и ServiceL)
  (AppL (run),
  ServiceL (startEndpoint httpCfg | grpcCfg)
  DbSrvL (create, createDb))

* (TODO) (проектирование/реализация) добавить bloom filter
    * спроектировать сценарии использования
    * реализовать
  
* (TODO) поддержи GRPC
  - при grpc уметь получать статистику по grpc

  
потом прикрутить SQL