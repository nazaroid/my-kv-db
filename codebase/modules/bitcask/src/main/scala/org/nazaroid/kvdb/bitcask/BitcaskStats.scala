package org.nazaroid.kvdb.bitcask

/**
 * Statistics structures for Bitcask hierarchy
 * No heterogeneous collections - concrete implementation
 */

// Statistics for single BitcaskTable
case class BitcaskTableStats(
  name: String,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  segmentCount: Int,
  activeSegmentCount: Int,
  segments: List[SegmentStats]
)

// Statistics for single BitcaskDatabase  
case class BitcaskDatabaseStats(
  name: String,
  totalTables: Int,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  totalSegments: Int,
  activeSegments: Int,
  tableStats: List[BitcaskTableStats]
)

// Statistics for BitcaskCatalog (all databases)
case class BitcaskCatalogStats(
  totalDatabases: Int,
  totalTables: Int,
  totalEntries: Int,
  activeEntries: Int,
  deletedEntries: Int,
  totalDataSize: Long,
  totalSegments: Int,
  activeSegments: Int
)

case class SegmentStats(
                         name:           String,
                         fileSize:       Long,
                         isActive:       Boolean,
                         staleDataRatio: Double,
                         entryCount:     Int)