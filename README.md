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


## TODO

(cv CrudSpec)

- (TODO) реализовать сценарий с удалением значения
- (TODO) проработать результаты CODE_REVIEW (особенно исправить баг по длинне ключей)
- (TODO) «v2 формата» с CRC и чёткой моделью сбоев (см ревью)

- (TODO) подготовить для OpenSource

- (TODO) добавить метрики
- 
- (TODO) MVP: попробовать переделать на DSL + Free (можно только AppL и ServiceL)
     (AppL (run), 
     ServiceL (startEndpoint httpCfg | grpcCfg)
     DbSrvL (create, createDb))

* (TODO) (проектирование/реализация) добавить bloom filter
  * спроектировать сценарии использования
  * реализовать

grafana + prometheus
https://github.com/gvolpe/trading/blob/main/docker-compose.yml

потом прикрутить SQL