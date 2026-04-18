# my-kv-db One-Pager

## Overview

`my-kv-db` is a small experimental key-value database built in Scala 3.
It combines a Bitcask-inspired storage engine with a simple HTTP API and built-in observability.
The codebase is written in a functional style, with Cats, Cats Effect, and FS2 used as core building blocks.

The project is designed as a learning-oriented, modular codebase that explores how a storage engine can evolve from binary file primitives into a usable service:

- append-only persistence
- per-segment indexing
- in-memory cache for fast reads
- database and table abstraction
- HTTP access for CRUD and statistics
- Prometheus metrics and Grafana playground

## Documentation

Extended project documentation lives in the [`docs/`](./docs/) directory.

- [`docs/index.md`](./docs/index.md) - documentation entry point and document map

Use this `README.md` as the project overview and continue with [`docs/index.md`](./docs/index.md) for architecture notes, playground setup, and internal supporting documents.

## Problem It Solves

The project targets a simple but useful scenario: persistent key-value storage for small services, prototypes, and infrastructure experiments where clarity of implementation matters as much as raw throughput.

Instead of starting from a large production-grade database, `my-kv-db` focuses on a transparent architecture that is easy to read, extend, test, and discuss in public.
It also serves as a compact example of applying functional programming patterns to storage and service design in Scala.

## Architecture Snapshot

The current design follows a three-level storage model:

1. `*.bin` data files store raw values in append-only format.
2. `*.idx` segment indexes map keys to offsets inside a segment.
3. `table.idx` stores the latest segment reference for each key and is used during recovery.

Core write path:

1. Append value to the data file.
2. Append key-to-offset mapping to the segment index.
3. Append key-to-segment mapping to the table index.
4. Update the in-memory cache after durable writes complete.

Core read path:

1. Resolve the key from the in-memory cache.
2. Return the cached live value or treat the key as absent.

Delete path:

1. Write a tombstone into the indexes.
2. Mark the key as deleted in memory.
3. Leave physical cleanup to compaction and segment maintenance.

The implementation also includes:

- segment rotation by size
- recovery on startup from persisted indexes
- compaction when segment count grows beyond configured limits
- statistics aggregation on catalog, database, table, and segment levels

## Module Layout

The repository is split into focused modules:

- `codebase/modules/core` - storage and statistics abstractions
- `codebase/modules/bin-file-io` - binary encoding, CRC, row read/write primitives
- `codebase/modules/bitcask` - Bitcask-style engine, database manager, table logic, stats adapter
- `codebase/modules/server` - HTTP server, routing, dependency composition
- `codebase/service` - runnable application entrypoint and config loading
- `codebase/modules/utils/metrics` - metrics exporter

This layout makes it possible to evolve the engine, transport layer, and tooling independently.

## Public API

The HTTP layer currently exposes:

- `POST /data/{db}` - create a database
- `POST /data/{db}/{table}` - create a table
- `POST /data/{db}/{table}/{key}` - store a value
- `GET /data/{db}/{table}/{key}` - read a value
- `DELETE /data/{db}/{table}/{key}` - delete a value
- `GET /health` - health check
- `GET /stats/catalog` - global statistics
- `GET /stats/database/{db}` - database statistics
- `GET /stats/table/{db}/{table}` - table statistics

The current public data model is intentionally simple: string keys and string values.

## Observability

`my-kv-db` is not only a storage experiment, but also an observability experiment.

The service exports Prometheus-compatible metrics and includes a local playground with:

- Prometheus scraping
- Grafana dashboards
- Docker Compose-based demo environment

This makes the project useful both as a storage prototype and as an example of how to instrument a stateful service.

## Current Status

The project is best described as an early open-source storage prototype:

- the Bitcask-inspired engine is implemented
- CRUD over HTTP is implemented
- statistics and metrics are implemented
- module boundaries are already visible
- the codebase still contains active TODOs around hardening, cleanup, and open-source packaging

That positioning is intentional: the repository is already useful for experimentation, design review, and contribution, while still being open to architectural improvement.

## Why Open Source

`my-kv-db` is a good open-source candidate because it sits at the intersection of several topics that are valuable to contributors:

- storage engine design
- binary file formats and indexes
- functional programming in Scala with Cats, Cats Effect, and FS2
- HTTP services with http4s
- service monitoring with Prometheus and Grafana

It is small enough to understand, but rich enough to discuss real trade-offs in durability, recovery, compaction, modularization, and API design.

## Contribution Themes

Good areas for public iteration include:

- durability modes and write guarantees
- recovery and snapshot optimization
- storage format validation and edge cases
- better module decomposition
- richer value types and serialization layers
- more transport options such as gRPC

## Summary

`my-kv-db` is a modular, Bitcask-inspired KV database project for Scala 3 that already demonstrates the full path from binary persistence to an observable HTTP service.
It is implemented in a functional style and uses the Cats ecosystem as a foundation for effects, composition, and concurrency.

It is not presented as a finished production database.
It is presented as a serious, readable, and extensible foundation for open-source collaboration.
