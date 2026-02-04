package org.nazaroid.kvdb.bitcasklib

final case class BitcaskConf(
  rootDir:              String = "kvdb",
  fileWriteParallelism: Int = 100,
  fileWriteBufferSize:  Int = 10000)
