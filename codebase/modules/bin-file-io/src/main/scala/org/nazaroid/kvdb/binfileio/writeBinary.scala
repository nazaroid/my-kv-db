package org.nazaroid.kvdb.binfileio

import cats.effect.{Async, Ref}
import cats.implicits.given
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.{Files, Flag, Flags, Path}

def writeBinary[F[_]: Async: Files](
  input:       Stream[F, WriteTask[F]],
  parallelism: Int = 100
): Stream[F,  Unit] = {

  final case class State(
    channels: Map[String, Channel[F, WriteTask[F]]],
    fifo:     List[String])

  Stream.eval(Ref.of[F, State](State(Map.empty, Nil))).flatMap { stateRef =>
    input
      .evalMap { task =>
        stateRef.modify { state =>
          state.channels.get(task.filePath) match {
            case Some(chan) =>
              (state, chan.send(task).void)

            case None =>
              // Evict the oldest channel if the parallelism limit is reached
              val (interimState, killAction) = if (state.channels.size >= parallelism) {
                val victim = state.fifo.head
                val victimChan = state.channels(victim)
                (state.copy(channels = state.channels - victim, fifo = state.fifo.tail), victimChan.close.void)
              } else {
                (state, Async[F].unit)
              }

              val setupNew = Channel.bounded[F, WriteTask[F]](1024).flatMap { newChan =>
                stateRef.update(s =>
                  s.copy(
                    channels = s.channels + (task.filePath -> newChan),
                    fifo     = s.fifo :+ task.filePath
                  )
                ) >>
                  Async[F]
                    .start {
                      // Determine the initial file size once when opening the channel
                      Files[F].size(Path(task.filePath)).handleError(_ => 0L).flatMap { initialSize =>
                        newChan
                          .stream
                          .evalMapAccumulate(initialSize) { (currentOffset, t) =>
                            val bytes = encode(t.row, t.schema)
                            val nextOffset = currentOffset + bytes.size

                            for {
                              // Write the specific row to disk
                              writeResult <- Stream
                                .chunk(bytes)
                                .through(Files[F].writeAll(Path(t.filePath), Flags(Flag.Create, Flag.Append)))
                                .compile
                                .drain
                                .attempt

                              result <- writeResult match {
                                case Right(()) =>
                                  // STRATEGIC MOMENT: notify Storage that the data is persisted on disk
                                  t.callback.traverse(_.complete(currentOffset)) *>
                                    Async[F].pure((nextOffset, Right(())))
                                case Left(error) =>
                                  t.callback.traverse(_.complete(-1L)) *>
                                    Async[F].pure((nextOffset, Left(error.getMessage)))
                              }
                            } yield result
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
                    }
                    .flatTap(_ => newChan.send(task))
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
