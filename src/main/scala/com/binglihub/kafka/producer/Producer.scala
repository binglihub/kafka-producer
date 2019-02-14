package com.binglihub.kafka.producer

import com.binglihub.kafka.producer.SupportedRecordFormat.SupportedRecordFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import com.binglihub.kafka.producer.Mode.Mode


object Producer extends App {


  val topic: String = args(0) //topic name
  val brokers: String = args(1) //broker url
  val recordSize: Int = args(2).toInt //number of each record batch
  val sleep: Long = args(3).toLong // sleep time between each record batch
  val repeat: Int = args(4).toInt // repeat time
  val recordFormat: SupportedRecordFormat = SupportedRecordFormat(args(5)) //output record format
  val mode: Mode = Mode(args(6)) //generating mode

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", CLIENT_ID)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)


  (0 until repeat)
    .foreach(i => {
      (0 until recordSize)
        .foreach(j => {
          println(s"${i}_${j}")
          producer.send(
            new ProducerRecord[String, String](topic, generateKey, generateValue(mode)
            )
          )
        })
      Thread.sleep(sleep)
    })

  producer.close()
}