# Architecture Notes

[Return to Index](./index.md)

Here is a summary of the current architecture of the storage library:

1. Storage Structure (3 layers)
   Data Layer (`.bin` files): Stores raw data values in segments. Writes are append-only.
   Segment Index (`.idx` files): A local index for each segment. Stores the mapping `Key -> Offset` inside a specific `.bin` file.
   Table Index (`table.idx`): A global registry for the table. Stores the mapping `Key -> SegmentName`. This is the source of truth during cold startup.

2. Main Processes
   Write Path: Writes happen in a cascade: Data -> Segment Index Offset -> Table Index Segment Name. After all three writes are confirmed, the in-memory cache is updated.
   Read Path: Reads always go through the cache (`O(1)`). If a key is missing from the cache, the system treats it as absent.
   Delete: A tombstone is written into the indexes and the key is marked as `Deleted` in the cache. Physical data is not removed until compaction.
   Rotation: When the size limit is reached, the manager creates a new pair of files (`.bin` and `.idx`) and switches active writes to them.

3. Background Maintenance
   Compaction: A merge process that collects all live records (not `Deleted`) from all segments, moves them into one new consolidated segment, and updates the global `table.idx`.
   Cleanup (GC): Deletes segment files from disk when no active cache entry references them anymore.
   Recovery: On startup, the system reads `table.idx`, resolves the current segment for each key, loads offsets from the segment `.idx` files, and warms up the cache.

4. Technology Stack
   Cats Effect: Effect management (`IO`, `Ref`, `Deferred`).
   FS2: Streaming file I/O and queue-based processing via `Channel`.
   Concurrency: Thread safety through `Ref` and serialized file writes through `writeBinary` workers.
