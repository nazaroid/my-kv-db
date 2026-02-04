package org.nazaroid.kvdb.binfileio

enum FieldType {
  case Int32
  case Int64
  case StringUtf8(sizeFromField: String)
}

case class FieldDef(name: String, fType: FieldType)

type Row = Map[String, Any]

final case class WriteTask(filePath: String, schema: List[FieldDef], row: Row)
