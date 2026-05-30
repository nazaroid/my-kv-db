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

## gpt promt
```
  В данном проекте я разрабатываю kv-базу даных с движком bitcask на Scala 3 + Cats. Структура проекта уже создана, все упоминаемые в задаче файлы уже существуют.
  Чтобы разобраться в структуре проекта изучи документацию, начиная с README.md и документы в папке docs .
  Код kv-базы находится в папке codebase. А в папке playground тестовая среда для обкатки.
```

## TODO

(cv CrudSpec)

+добавить метрики по скорости запись/чтение
  экспортер для прометеуса
  набросок в AI:
  https://www.google.com/search?sourceid=chrome&udm=50&aep=42&q=%D0%AF+%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%B0%D1%82%D1%8B%D0%B2%D0%B0%D1%8E+%D0%BF%D1%80%D0%BE%D1%81%D1%82%D0%B5%D0%BD%D1%8C%D0%BA%D1%83%D1%8E+kv-%D0%B1%D0%B0%D0%B7%D1%83+%D0%B4%D0%B0%D0%BD%D1%8B%D1%85+%D1%81+%D0%B4%D0%B2%D0%B8%D0%B6%D0%BA%D0%BE%D0%BC+bitcask.%0A%D0%A3+%D0%BC%D0%B5%D0%BD%D1%8F+%D0%BE%D0%BF%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D0%B8+%D0%B7%D0%B0%D0%BF%D0%B8%D1%81%D0%B8+%D0%B8+%D1%87%D1%82%D0%B5%D0%BD%D0%B8%D1%8F%2C+%D0%BD%D0%BE+%D0%BD%D0%B5%D1%82+%D0%BD%D0%B8%D0%BA%D0%B0%D0%BA%D0%BE%D0%B9+%D1%81%D1%82%D0%B0%D1%82%D0%B8%D1%81%D1%82%D0%B8%D0%BA%D0%B8.+%D0%9F%D1%80%D0%B5%D0%B4%D0%BB%D0%BE%D0%B6%D0%B8+%D0%BA%D0%B0%D0%BA+%D0%B8%D1%81%D0%BF%D1%80%D0%B0%D0%B2%D0%B8%D1%82%D1%8C+%D1%81%D0%B8%D1%82%D1%83%D0%B0%D1%86%D0%B8%D1%8E&mstk=AUtExfBFmzELB1g_0ahRd1paz1U43w31fV6fGYPUJbiKTcJNgHlD191coevSJ0LcKsdnD8NJPjYuZGxg1Xha6mUpQWXk2mg3RQ2sgLx52ePnmCcf5BeTtd8g_8SpskKt3LQ0KDTCkQLg4BFd-2xyCkcYDwveSWNwTMoaqJEj4ERSZtuLT4IDp0ed8vmMwcrnR7s0wmVupLg3NvIKFZS3_kcuO85FwL14TSD2xs8rpjt5Rmc0GBcehnZYwKyLAw&csuir=1
  В данном проекте я разрабатываю kv-базу даных с движком bitcask на Scala 3 + Cats. Структура проекта уже создана, все упоминаемые в задаче файлы уже существуют.
  Чтобы разобраться в структуре проекта изучи документацию, начиная с README.md и документы в папке docs .
  Код kv-базы находится в папке codebase. А в папке playground тестовая среда для обкатки.
  У меня имеются операции get, set и delete в codebase/modules/bitcask/src/main/scala/org/nazaroid/kvdb/bitcask/BitcaskEngine.scala, но нет метрик на эти операции.
  Добавь их поддержку для BitcaskEngine и выведи на дашборд Grafana в playground
  

- (TODO): /refact/ декомпозировать на модули
  - выделить два основных компонента и слои: 
    - storage:
      - server
      - engine
      - statistics
    - shared
      - utils
    - node

- (TODO): /refact/ выделить MonitoringService
  - StatisticsService: выделить MonitoringService 
    - рассмотреть как можно было бы сделать событийную модель. Например, передавать два Action (регистрация и обновление) в startMonitoring
    - metricsAdapter.registerMetrics() ничего не делает. Можно выделить метрики в case class и в registerMetrics его устаналивать в состояие
  
- (TODO): /refact/ разделить утилиты
  - для инстанса DB не нужен [metrics](codebase/modules/utils/metrics)
    нужен только для http-сервера: [prometheus](codebase/modules/utils/third_party/prometheus)

- (TODO): /refact/ пересмотреть связи между StatisticsService <-> DatabaseManager[F] <-> Engine
  - сейчас внедрение DatabaseManager в StatisticsService через engine.dbManager
  - как можно переделать: DatabaseManager трансформировать в Catalog, т.к это аналог BitcaskCatalog (его нужно переименовать так из Catalog) и сделать его враппером над BitcaskCatalog
  
- (TODO): вынести в конфиг
  - c <- Catalog.init(Path(rootPath), configTemplate, 1024, 2)

- (TODO) оптимизировать: size <- Files[F].size(Path(ds.filePath)).handleError(_ => 0L)
  - вроде уже оптимизировано

- (TODO) проработать результаты CODE_REVIEW (особенно исправить баг по длинне ключей)
  - конкуретная запись на уровне сегментов
  - избавиться от `encoding` бросает `throw`
  - ввести конфиг `durability: Relaxed | FsyncPerSegment | FsyncPerBatchвести конфиг `durability: Relaxed | FsyncPerSegment | FsyncPerBatch
  - длины ключей считаются в символах, а не в байтах UTF-8

* (TODO) добавить типы
  Blob / Byte Array - базовый тип для хранения
      Bitcask Engine: возвращает Byte Array
      Serialization/Codec Layer: работает поверх engine и производит преобразования
  String (Строка): Основной тип для ключей. Позволяет организовывать пространство имен (например, user:123:profile) и выполнять поиск по маскам.
  Integer (Целое число): Необходим для реализации атомарных счетчиков и инкрементальных операций без полной перезаписи значения.
  Boolean (Логический тип): Часто используется для флагов состояния, так как занимает минимум места.

- (TODO) подготовить для OpenSource
  - README.md
    - дополнить ссылкой на docs
    - логотип
  - провести Code Review и провести рефакторинги

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