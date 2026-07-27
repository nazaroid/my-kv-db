package org.nazaroid.kvdb.bitcask

final case class BitcaskEngineConfig(
  rootDir:              String = "./testFolder",
  fileWriteParallelism: Int = 10,
  fileWriteBufferSize:  Int = 10000,
  maxSegmentSize:       Long = 1024 * 10,
  maxSegmentCount:      Int = 10,
  maxRetries:           Int = 3,
  failureRecovery:      Boolean = true)
