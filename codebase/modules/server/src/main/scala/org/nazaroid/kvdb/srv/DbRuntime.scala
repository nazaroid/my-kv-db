package org.nazaroid.kvdb.srv

import cats.effect.unsafe.IORuntime
import io.prometheus.client.CollectorRegistry

import java.util.concurrent.atomic.AtomicReference

final class DbRuntime(
  val io:      IORuntime = IORuntime.builder().build(),
  val stopActionRef: AtomicReference[() => Unit] = new AtomicReference[() => Unit](() => ())) {

  def shutdown(): Unit = {
    {
      CollectorRegistry.defaultRegistry.clear()
      stopActionRef.get().apply()
      io.shutdown()
    }
  }

}
