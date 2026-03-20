package org.nazaroid.kvdb.bitcask.catalog

import org.nazaroid.kvdb.bitcask.storage.StorageManager

type BitcaskTable[F[_]] = StorageManager[F]
