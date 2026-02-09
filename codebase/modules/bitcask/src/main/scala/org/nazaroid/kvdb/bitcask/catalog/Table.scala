package org.nazaroid.kvdb.bitcask.catalog

import org.nazaroid.kvdb.bitcask.storage.StorageManager

type Table[F[_]] = StorageManager[F]
