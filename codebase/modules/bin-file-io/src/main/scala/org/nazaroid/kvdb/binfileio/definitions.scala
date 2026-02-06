package org.nazaroid.kvdb.binfileio

import cats.effect.*
import fs2.io.file.Files

enum FieldType {
  case Int32
  case Int64
  case StringUtf8(sizeFromField: String)
}

case class FieldDef(name: String, fType: FieldType)

type Row = Map[String, Any]

final case class WriteTask[F[_]: Async: Files](
  key:      String, // ID для индекса
  filePath: String,
  schema:   List[FieldDef],
  row:      Row,
  callback: Option[Deferred[F, Long]] = None)
