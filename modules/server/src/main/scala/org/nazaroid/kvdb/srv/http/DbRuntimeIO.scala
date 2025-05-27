package org.nazaroid.kvdb.srv.http

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.prometheus.client.CollectorRegistry
import org.nazaroid.kvdb.algebra.DbRuntime

final class DbRuntimeIO(val io: IORuntime = IORuntime.builder().build()) extends DbRuntime[IO] {

  override def shutdown(): Unit = {
    {
      CollectorRegistry.defaultRegistry.clear()
      io.shutdown()
    }
  }

}

object DbRuntimeIO {
  given DbRuntimeIO = new DbRuntimeIO(IORuntime.builder().build())
}
