package org.nazaroid.kvdb.bitcask.catalog

import org.nazaroid.kvdb.binfileio.StorageManager

type Table[F[_]] = StorageManager[F]
