package org.nazaroid.kvdb.srv.http

import org.nazaroid.kvdb.algebra.{DbServer, DbServerHandle}

final class HttpDbServer[F[_]] extends DbServer[F] {

  override def run(): F[DbServerHandle[F]] = ???
}
