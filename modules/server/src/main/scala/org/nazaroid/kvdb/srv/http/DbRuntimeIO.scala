package org.nazaroid.kvdb.srv.http

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import org.nazaroid.kvdb.algebra.DbRuntime

import scala.concurrent.ExecutionContext

final class DbRuntimeIO(val io: IORuntime, val ec: ExecutionContext) extends DbRuntime[IO] {

  override def shutdown(): Unit = {
    {
      io.shutdown()
    }
  }

}
