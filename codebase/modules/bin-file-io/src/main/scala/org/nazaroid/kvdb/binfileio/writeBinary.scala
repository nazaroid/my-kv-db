package org.nazaroid.kvdb.binfileio

import cats.effect.*
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.Channel
import fs2.io.file.{Files, Flag, Flags, Path}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

def writeBinary[F[_]: Async: Files](
                                     input: Stream[F, WriteTask],
                                     parallelism: Int = 100
                                   ): Stream[F, Unit] = {

  final case class State(
                          channels: Map[String, Channel[F, WriteTask]],
                          fifo: List[String]
                        )

  Stream.eval(Ref.of[F, State](State(Map.empty, Nil))).flatMap { stateRef =>
    input
      .evalMap { task =>
        stateRef.modify { state =>
          state.channels.get(task.filePath) match {
            case Some(chan) =>
              (state, chan.send(task).void)

            case None =>
              val (interimState, killAction) = if (state.channels.size >= parallelism) {
                val victim = state.fifo.head
                val victimChan = state.channels(victim)
                (state.copy(channels = state.channels - victim, fifo = state.fifo.tail), victimChan.close.void)
              } else {
                (state, Async[F].unit)
              }

              val setupNew = Channel.bounded[F, WriteTask](1024).flatMap { newChan =>
                stateRef.update(s =>
                  s.copy(
                    channels = s.channels + (task.filePath -> newChan),
                    fifo     = s.fifo :+ task.filePath
                  )
                ) >>
                  Async[F].start {
                    // Узнаем размер файла один раз при открытии канала
                    Files[F].size(Path(task.filePath)).handleError(_ => 0L).flatMap { initialSize =>
                      newChan.stream
                        .evalMapAccumulate(initialSize) { (currentOffset, t) =>
                          val bytes = rowToBytes(t.row, t.schema)
                          val nextOffset = currentOffset + bytes.size

                          // Записываем конкретную строку
                          Stream.chunk(bytes)
                            .through(Files[F].writeAll(Path(t.filePath), Flags(Flag.Append)))
                            .compile
                            .drain >>
                            // СТРАТЕГИЧЕСКИЙ МОМЕНТ: уведомляем Storage, что данные на диске
                            t.callback.traverse(_.complete(currentOffset)) >>
                            Async[F].pure((nextOffset, ()))
                        }
                        .onFinalize {
                          stateRef.update(s =>
                            s.copy(
                              channels = s.channels - task.filePath,
                              fifo     = s.fifo.filterNot(_ == task.filePath)
                            )
                          )
                        }
                        .compile
                        .drain
                    }
                  }.flatTap(_ => newChan.send(task))
              }

              (interimState, killAction >> setupNew.void)
          }
        }.flatten
      }
      .onFinalize {
        stateRef.get.flatMap(_.channels.values.toList.traverse(_.close).void)
      }
  }
}
