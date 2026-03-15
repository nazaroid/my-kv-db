package org.nazaroid.kvdb.bitcask.storage

// Database statistics case classes
case class DatabaseStats(
  totalTables: Int,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  tableStats: List[TableStats],
  segmentStats: List[SegmentStats]
)

case class TableStats(
  name: String,
  entryCount: Int,
  activeEntryCount: Int
)

case class SegmentStats(
  name: String,
  fileSize: Long,
  isActive: Boolean,
  staleDataRatio: Double,
  entryCount: Int
)
