package org.nazaroid.kvdb.srv.http

import cats.effect.unsafe.IORuntime
import io.prometheus.client.CollectorRegistry

final class DbRuntime(val io: IORuntime = IORuntime.builder().build()) {

  def shutdown(): Unit = {
    {
      CollectorRegistry.defaultRegistry.clear()
      io.shutdown()
    }
  }

}
