package com.binglihub.kafka.producer

object Mode extends Enumeration {
  type Mode = Value
  val RAND_1, AVRO_GROUP = Value

  def apply(mode: String): Mode =
    mode.toLowerCase match {
      case "rand_1" => RAND_1
      case "avro_group" => AVRO_GROUP
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported mode $mode")
    }
}
