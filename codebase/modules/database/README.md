# Database Module

This module provides abstract interfaces for database management and breaks circular dependencies between server and statistics modules.

## Architecture

### Module Dependencies (No Circular Dependencies)

```
┌─────────────┐    depends on    ┌─────────────┐
│   server    │ ◄────────────── │  database   │
└─────────────┘                └─────────────┘
       ▲                               ▲
       │ depends on                    │ depends on
┌─────────────┐                ┌─────────────┐
│ statistics  │ ◄────────────── │  database   │
└─────────────┘                └─────────────┘
                                      ▲
                                      │ depends on
                              ┌─────────────┐
                              │   bitcask   │
                              └─────────────┘
```

### Key Components

#### 1. DatabaseManager[F]
Abstract interface for managing multiple databases:
```scala
trait DatabaseManager[F[_]] {
  def createDatabase(name: String): F[Database[F]]
  def getDatabase(name: String): F[Option[Database[F]]]
  def listDatabases: F[List[String]]
  def deleteDatabase(name: String): F[Unit]
  def getStats: F[DatabaseStats]
}
```

#### 2. Engine[F]
Abstract engine interface that works with DatabaseManager:
```scala
trait Engine[F[_]] {
  def createDbIfNotExists(name: String): F[Unit]
  def createTableIfNotExists(baseName: String, tblName: String): F[Unit]
  def get(baseName: String, tblName: String, key: String): F[Option[String]]
  def set(baseName: String, tblName: String, key: String, value: String): F[Unit]
  def delete(baseName: String, tblName: String, key: String): F[Unit]
  def getStats: F[DatabaseStats]
}
```

#### 3. Database[F] and Table[F]
Abstract interfaces for database and table operations:
```scala
trait Database[F[_]] {
  def name: String
  def createTable(name: String): F[Unit]
  def getTable(name: String): F[Option[Table[F]]]
  def listTables: F[List[String]]
  def deleteTable(name: String): F[Unit]
}

trait Table[F[_]] {
  def name: String
  def get(key: String): F[Option[String]]
  def set(key: String, value: String): F[Unit]
  def delete(key: String): F[Unit]
  def listKeys: F[List[String]]
}
```

## Bitcask Implementation

### BitcaskDatabaseManager[F]
Concrete implementation for Bitcask storage engine:
```scala
class BitcaskDatabaseManager[F[_]: Async: Files: Logger](
  rootPath: String
) extends DatabaseManager[F]
```

### BitcaskEngine[F]
Concrete engine implementation:
```scala
final class BitcaskEngine[F[_]: Async: Logger](
  databaseManager: BitcaskDatabaseManager[F]
) extends Engine[F]
```

## Usage

### Creating an Engine
```scala
val engineConfig = EngineConfig(
  rootDir = "/tmp/kvdb",
  maxSegmentSize = 1024 * 1024,
  maxSegmentCount = 10,
  fileWriteBufferSize = 1024,
  fileWriteParallelism = 2
)

val engine: Resource[F, Engine[F]] = BitcaskEngine.init[F](engineConfig)
```

### Using the Engine
```scala
// Create database and table
engine.createDbIfNotExists("users")
engine.createTableIfNotExists("users", "profiles")

// Store and retrieve data
engine.set("users", "profiles", "user1", "John Doe")
val value = engine.get("users", "profiles", "user1")

// Get statistics
val stats = engine.getStats
```

## Benefits

### 1. No Circular Dependencies
- Server module depends on database module
- Statistics module depends on database module
- Database module depends on bitcask module
- Clean dependency graph

### 2. Proper Abstraction
- Engine works with databases, not single tables
- StatisticsService works with multiple databases
- Clean separation of concerns

### 3. Extensibility
- Easy to add new engine implementations
- Statistics can work with any database manager
- Server can use any engine implementation

### 4. Type Safety
- Abstract interfaces prevent implementation leakage
- Generic types ensure compile-time safety
- Heterogeneous statistics details maintain flexibility

## Migration from Old Architecture

### Before (Circular Dependencies)
```
server → statistics → StorageManager → Engine → server
```

### After (Clean Dependencies)
```
server → database → bitcask
statistics → database → bitcask
```

This module resolves the architectural issues while maintaining all functionality.
