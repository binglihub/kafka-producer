package com.binglihub.kafka.producer

object SupportedRecordFormat extends Enumeration {
  type SupportedRecordFormat = Value
  val JSON, AVRO/*CSV*/ = Value

  def apply(formatName: String): SupportedRecordFormat =
    formatName.toLowerCase match {
      case "json" => JSON
      case "avro" => AVRO
//      case "csv" => CSV
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported file format $formatName")
    }
}
