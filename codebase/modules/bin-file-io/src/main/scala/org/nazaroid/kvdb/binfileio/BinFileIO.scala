package org.nazaroid.kvdb.binfileio

import cats.effect.IO

object BinFileIO:
  export org.nazaroid.kvdb.binfileio.writeBinary as write
  export org.nazaroid.kvdb.binfileio.readBinary as read