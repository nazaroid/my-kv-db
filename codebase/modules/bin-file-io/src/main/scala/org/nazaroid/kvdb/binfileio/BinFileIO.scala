package org.nazaroid.kvdb.binfileio

object BinFileIO:
  export org.nazaroid.kvdb.binfileio.writeBinary as writeAll
  export org.nazaroid.kvdb.binfileio.readBinary as readAll
  export org.nazaroid.kvdb.binfileio.readRowAt as readSingleAt
  export org.nazaroid.kvdb.binfileio.encode as rowToBytes
