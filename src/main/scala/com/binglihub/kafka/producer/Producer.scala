package com.binglihub.kafka.producer

import com.binglihub.kafka.producer.SupportedRecordFormat.SupportedRecordFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import com.binglihub.kafka.producer.Mode.Mode

import org.apache.avro.Schema


object Producer extends App {

  val props = new Properties()

  props.put("bootstrap.servers", args(1)) //broker url
  props.put("client.id", CLIENT_ID)

  val topic: String = args(0) //topic name
  val recordSize: Int = args(2).toInt //number of each record batch
  val sleep: Long = args(3).toLong // sleep time between each record batch
  val repeat: Int = args(4).toInt // repeat time
  val recordFormat: SupportedRecordFormat = SupportedRecordFormat(args(5)) //output record format
  val mode: Mode = Mode(args(6)) //generating mode

  lazy val jsonProducer = new KafkaProducer[String, String](props)
  lazy val avroProducer = new KafkaProducer[String, Array[Byte]](props)

  recordFormat match {
    case SupportedRecordFormat.JSON => {
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    }
    case SupportedRecordFormat.AVRO => {
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    }
  }

  //avro
  val parser = new Schema.Parser
  val schemaJson =
    """
      |{
      |"name":"test_avro",
      |"type":"record",
      |"fields":[
      | {"name":"int","type":"int"},
      | {"name":"long","type":"long"},
      | {"name":"double","type":"double"},
      | {"name":"string","type":"string"},
      | {"name":"boolean","type":"boolean"}
      |]
      |}
    """.stripMargin

  val schema = parser.parse(schemaJson)


  (0 until repeat)
    .foreach(i => {
      (0 until recordSize)
        .foreach(j => {
          println(s"${i}_${j}")
          recordFormat match {
            case SupportedRecordFormat.JSON =>
              jsonProducer.send(
                new ProducerRecord[String, String](topic, generateKey, generateJsonValue(mode)))

            case SupportedRecordFormat.AVRO =>
              avroProducer.send(
                new ProducerRecord[String, Array[Byte]](topic, generateKey, generateAvroValue(mode, schema)))
          }
        })
      Thread.sleep(sleep)
    })

  recordFormat match {
    case SupportedRecordFormat.JSON => jsonProducer.close()
    case SupportedRecordFormat.AVRO => avroProducer.close()
  }
}