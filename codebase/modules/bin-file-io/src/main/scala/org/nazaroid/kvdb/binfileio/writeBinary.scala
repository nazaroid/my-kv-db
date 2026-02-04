package org.nazaroid.kvdb.binfileio

import cats.effect.*
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.Channel
import fs2.io.file.{Files, Flag, Flags, Path}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

def writeBinary[F[_]: Async: Files](input: Stream[F, WriteTask], parallelism: Int = 100): Stream[F, Unit] = {

  def rowToBytes(row: Row, schema: List[FieldDef]): Chunk[Byte] = {
    val bv = schema.foldLeft(ByteVector.empty) { (acc, field) =>
      val value = row.getOrElse(field.name, throw new Exception(s"Field ${field.name} missing"))
      val fieldBytes = field.fType match {
        case FieldType.Int32 => ByteVector.fromInt(value.asInstanceOf[Int])
        case FieldType.Int64 => ByteVector.fromLong(value.asInstanceOf[Long])
        case FieldType.StringUtf8(_) =>
          ByteVector.view(value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
      }
      acc ++ fieldBytes
    }
    Chunk.byteVector(bv)
  }

  final case class State(
    channels: Map[String, Channel[F, WriteTask]],
    fifo:     List[String])

  Stream.eval(Ref.of[F, State](State(Map.empty, Nil))).flatMap { stateRef =>
    input
      .evalMap { task =>
        stateRef.modify { state =>
          state.channels.get(task.filePath) match {
            case Some(chan) =>
              // File is active: send task to channel
              (state, chan.send(task).void)

            case None =>
              // New file: check for eviction
              val (interimState, killAction) = if (state.channels.size >= parallelism) {
                val victim = state.fifo.head
                val victimChan = state.channels(victim)
                // Closing the channel finishes its stream gracefully
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
                  // Create the writer fiber
                  Async[F]
                    .start {
                      newChan
                        .stream
                        .map(t => rowToBytes(t.row, task.schema))
                        .unchunks
                        .through(
                          Files[F].writeAll(
                            Path(task.filePath),
                            Flags(Flag.Create, Flag.Write, Flag.Append)
                          )
                        )
                        .onFinalize {
                          // Correct: onFinalize is called on the Stream
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
                    .flatTap(_ => newChan.send(task))
              }

              (interimState, killAction >> setupNew.void)
          }
        }.flatten
      }
      .onFinalize {
        // Cleanup: close all active channels when input stream ends
        stateRef.get.flatMap(_.channels.values.toList.traverse(_.close).void)
      }
  }
}
