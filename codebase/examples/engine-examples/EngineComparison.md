# Engine Statistics Comparison

This document demonstrates how different database engines can provide heterogeneous statistics through the unified `Engine[F]` interface.

## Abstract Interface

```scala
trait Engine[F[_]] {
  def createDbIfNotExists(name: String): F[Unit]
  def createTableIfNotExists(baseName: String, tblName: String): F[Unit]
  def get(baseName: String, tblName: String, key: String): F[Option[String]]
  def set(baseName: String, tblName: String, key: String, value: String): F[Unit]
  def delete(baseName: String, tblName: String, key: String): F[Unit]
  
  def getStats: F[DatabaseStats]  // Unified interface
}

case class DatabaseStats(
  totalTables: Int,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  details: Map[String, Json]  // Heterogeneous collection
)
```

## Bitcask Engine

**Architecture:** Log-structured with segments on disk

```json
{
  "totalTables": 3,
  "totalEntries": 1250,
  "activeEntries": 1200,
  "deletedEntries": 50,
  "totalDataSize": 1048576,
  "details": {
    "engine_type": "bitcask",
    "table_stats": [
      {
        "name": "users",
        "entry_count": 800,
        "active_entry_count": 780
      },
      {
        "name": "sessions",
        "entry_count": 450,
        "active_entry_count": 420
      }
    ],
    "segment_stats": [
      {
        "name": "seg_1647891234567",
        "file_size": 262144,
        "is_active": true,
        "stale_data_ratio": 0.1,
        "entry_count": 200
      },
      {
        "name": "seg_1647890000000",
        "file_size": 524288,
        "is_active": false,
        "stale_data_ratio": 0.7,
        "entry_count": 400
      }
    ],
    "segment_count": 2,
    "active_segment_count": 1
  }
}
```

## In-Memory Engine

**Architecture:** Pure in-memory storage

```json
{
  "totalTables": 2,
  "totalEntries": 500,
  "activeEntries": 500,
  "deletedEntries": 0,
  "totalDataSize": 50000,
  "details": {
    "engine_type": "in_memory",
    "memory_usage_mb": 128,
    "max_memory_mb": 1024,
    "database_count": 2,
    "average_entries_per_db": 250,
    "persistence": false,
    "compression": false
  }
}
```

## Single-File Engine

**Architecture:** All data in one serialized file

```json
{
  "totalTables": 1,
  "totalEntries": 100,
  "activeEntries": 100,
  "deletedEntries": 0,
  "totalDataSize": 8192,
  "details": {
    "engine_type": "single_file",
    "file_path": "/data/kvdb.dat",
    "file_size_bytes": 8192,
    "file_size_mb": 0.008,
    "serialization_format": "java_serialization",
    "compression": false,
    "backup_count": 0,
    "last_modified": 1647891234567
  }
}
```

## Distributed Engine (Future)

**Architecture:** Multiple nodes with sharding

```json
{
  "totalTables": 5,
  "totalEntries": 10000,
  "activeEntries": 9500,
  "deletedEntries": 500,
  "totalDataSize": 10485760,
  "details": {
    "engine_type": "distributed",
    "node_count": 3,
    "shard_count": 6,
    "replication_factor": 2,
    "nodes": [
      {
        "id": "node-1",
        "role": "primary",
        "shards": ["shard-1", "shard-2"],
        "entries": 3500,
        "data_size_mb": 3.5
      },
      {
        "id": "node-2", 
        "role": "primary",
        "shards": ["shard-3", "shard-4"],
        "entries": 3500,
        "data_size_mb": 3.5
      },
      {
        "id": "node-3",
        "role": "replica",
        "shards": ["shard-5", "shard-6"],
        "entries": 3000,
        "data_size_mb": 3.0
      }
    ],
    "network_latency_ms": 2.5,
    "consistency_level": "eventual"
  }
}
```

## Column-Based Engine (Future)

**Architecture:** Column-oriented storage

```json
{
  "totalTables": 4,
  "totalEntries": 50000,
  "activeEntries": 48000,
  "deletedEntries": 2000,
  "totalDataSize": 20971520,
  "details": {
    "engine_type": "column_based",
    "compression": "snappy",
    "column_stats": [
      {
        "name": "id",
        "type": "integer",
        "size_bytes": 200000,
        "compression_ratio": 0.3,
        "null_ratio": 0.0
      },
      {
        "name": "name",
        "type": "string",
        "size_bytes": 800000,
        "compression_ratio": 0.6,
        "null_ratio": 0.1
      },
      {
        "name": "created_at",
        "type": "timestamp",
        "size_bytes": 400000,
        "compression_ratio": 0.4,
        "null_ratio": 0.0
      }
    ],
    "index_count": 2,
    "materialized_view_count": 1
  }
}
```

## Benefits of Heterogeneous Approach

1. **Flexibility**: Each engine can expose its specific metrics
2. **Extensibility**: New engines can add new fields without breaking existing code
3. **Type Safety**: JSON provides runtime type safety for heterogeneous data
4. **Discovery**: Clients can discover engine capabilities at runtime
5. **Monitoring**: Different monitoring systems can extract relevant metrics

## Usage Examples

```scala
// Client code works with any engine
val stats = engine.getStats
println(s"Total entries: ${stats.totalEntries}")

// Engine-specific details
stats.details.get("engine_type") match {
  case Some(Json.fromString("bitcask")) =>
    val segmentCount = stats.details("segment_count").asNumber.toInt
    println(s"Segments: $segmentCount")
    
  case Some(Json.fromString("in_memory")) =>
    val memoryUsage = stats.details("memory_usage_mb").asNumber.toInt
    println(s"Memory usage: ${memoryUsage}MB")
    
  case Some(Json.fromString("distributed")) =>
    val nodeCount = stats.details("node_count").asNumber.toInt
    println(s"Nodes: $nodeCount")
}
```

This approach maintains clean abstraction while allowing each engine to provide its specific statistics.
