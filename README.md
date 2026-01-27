# my-kv-db

## refs
https://drive.google.com/drive/u/1/folders/1BpJo8j4xkWuLI-9Zls8dZNmtWIdH_zwQ
https://docs.google.com/document/d/1sww6oOqIgJuGDw4UU746usO1lyFgargHtrjEj3av__k/edit?tab=t.0
https://app.diagrams.net/?libs=general;citrix#

https://www.youtube.com/watch?v=NITGB4SH-j8&t=335s
https://github.com/itmo-java-basics-2021/lab-base-code/tree/master/src/main/java/com/itmo/java

crc - Циклический избыточный код (англ. Cyclic redundancy check[нет в источнике], CRC[1]) — алгоритм нахождения контрольной суммы, предназначенный для проверки целостности данных[2].
https://ru.wikipedia.org/wiki/%D0%A6%D0%B8%D0%BA%D0%BB%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8%D0%B9_%D0%B8%D0%B7%D0%B1%D1%8B%D1%82%D0%BE%D1%87%D0%BD%D1%8B%D0%B9_%D0%BA%D0%BE%D0%B4
в bitcast считается для каждой строки (см bitcask-intro.pdf на google диске)


## Архитектура
- уровень server: http/grpc
-- (позже) Уровень запросов: SQL/JSON 
- сервисный уровень: API: инстанс
- движок (файловый уровень) : модуль bitcask
-- DSL + интерпретатор
- https://typelevel.org/cats/datatypes/freemonad.html
- App Layer
- Server Layer
- Svc Layer
- Persistence Layer
  https://gitlab.com/VictorWinbringer/ddd_scala/-/blob/main/src/main/scala/vw/ddd_scala/core/domain/services/UuidsRepository.scala?ref_type=heads
-- гексагональная арх-ра

## TODO

(cv CrudSpec)

* реализовать сценарий create_db, create_tb, write, read
  * база данных и таблица - это папки
  * segment - это append only - file
  * table index - получить сегмент по ключу (Key -> Segment)
  * segment index - получить offset в segment по ключу ( (Segment, Key) -> SegmentOffsetInfo)
  * Замечание: в bitcast вместо этих двух индексов есть индкекс keydir: giving the file, offset, and size of the most recently
    written entry for that key


  (+) выделить слои (Config, Env, Di)
     App  
     Server (Http / Grpc) 
     Engine (BitCaskDbEngine)

(+) реализовать HttpDbServer
   - POST 201 http://$host:$port/data/db
   - POST 201 http://$host:$port/data/db/tbl
   - POST http://$host:$port/data/db/key
   - GET  http://$host:$port/data/db/key
     (+) сделать Engine на Map[String, String]

  потом начать делать BitCask
   - (+) Проектирование: 
     - описать систему классов
     - описать файловую структуру (все файлы и папки)
       -- обдумать какое содержимое индексов и описать каждый
       -- см раздел Индексирвание в файле на GoogleDisk
   - (+) реализация get/set

- (+) рефакт: декомпозировть модуль server на части
    - например:db, server, engine http,  bitcask
      - app
        
      - db
        - (opt) config
          - возможно конфиги лучше в server
        - server
          - http
          - grpc
        - transact
            - fs2 cmd interface
        - engine
          - ddl, dml
        - bitcask
          - BitcaskLib
          - algebra
            - ...
          - instances
            - ...
        - metrics
    - отдельно fileformat (Persistence)

- (+) рефакт: декомпозировть BitcaskLib
    - (сейчас все в одном файле BitcaskLib)
    - 
- (! TODO) рефакт: выделить слой хранения в бинарном виде BinReader
    - см файл BinReader

- (! TODO) (IN PROGRESS)  восстановление кеша при старте
  - (читаем структуру и загружаем граф сущностей в кеше)
  - план:
  - прочитать файловую струткуру (придумать название.):
    - получить список баз и внутри них списки таблиц+сегментов+индексов
  - наполнить кеш сервис: 
    - создать сущности, из файлов (индексы + сегменты) 
    - наполнить свойства data (см src/main/scala/org/nazaroid/kvdb/bitcask/lib/algebra/package.scala)
- (! TODO) логирование

* (! TODO)  реализовать сценарий с удалением значения

(! TODO) (проектирование/реализация) сделать в engine приемку команд при помощи fs2
- модуль transact
** public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command)

(! TODO) MVP: попробовать переделать на DSL + Free (можно только AppL и ServiceL)
     (AppL (run), 
     ServiceL (startEndpoint httpCfg | grpcCfg)
     DbSrvL (create, createDb))

* (! TODO)  (проектирование/реализация) добавить bloom filter
  * спроектировать сценарии использования
  * реализовать





grafana + prometheus
https://github.com/gvolpe/trading/blob/main/docker-compose.yml

потом прикрутить SQL