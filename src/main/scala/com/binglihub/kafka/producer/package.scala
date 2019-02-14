package com.binglihub.kafka

import com.binglihub.kafka.producer.Mode.Mode
import com.fasterxml.jackson.databind.ObjectMapper

import scala.util.Random

package object producer {

  val CLIENT_ID: String = "snowflake_test_kafka_producer"

  val mapper = new ObjectMapper()

  def generateKey: String = Random.alphanumeric take 10 mkString

  def generateValue(mode: Mode): String =
    mode match {
      case Mode.RAND_1 => //int,long,double,text,boolean
        val obj = mapper.createObjectNode()
        obj.put("int", Random.nextInt())
        obj.put("long", Random.nextLong())
        obj.put("double", Random.nextDouble())
        obj.put("text", Random.nextString(20))
        obj.put("boolean", Random.nextBoolean())
        obj.toString
    }
}
