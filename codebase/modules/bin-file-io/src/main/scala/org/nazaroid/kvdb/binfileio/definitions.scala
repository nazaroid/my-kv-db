package org.nazaroid.kvdb.binfileio

import cats.effect.{Async, Deferred}
import fs2.io.file.Files

enum FieldType {
  case Int32
  case Int64
  case StringUtf8(sizeFromField: String)
}

case class FieldDef(name: String, fType: FieldType)

type Row = Map[String, Any]

case class WriteTask[F[_]: Async: Files](
  id:       String,
  filePath: String,
  schema:   List[FieldDef],
  row:      Row,
  callback: Option[Deferred[F, Long]])
