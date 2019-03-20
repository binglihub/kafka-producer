package com.binglihub.kafka

import java.io.ByteArrayOutputStream

import com.binglihub.kafka.producer.Mode.Mode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory

import scala.util.Random

package object producer {

  val CLIENT_ID: String = "snowflake_test_kafka_producer"

  val mapper = new ObjectMapper()

  def generateKey: String = Random.alphanumeric take 10 mkString

  def generateJsonValue(mode: Mode): String =
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

  def generateAvroValue(mode: Mode, schema: Schema): Array[Byte] =
    mode match {
      case Mode.RAND_1 =>
        val record = new GenericData.Record(schema)
        record.put("int", Random.nextInt())
        record.put("long", Random.nextLong())
        record.put("double", Random.nextDouble())
        record.put("string", Random.nextString(20))
        record.put("boolean", Random.nextBoolean())

        val writer = new GenericDatumWriter[GenericRecord](schema)
        val output = new ByteArrayOutputStream()
        val fileWriter = new DataFileWriter[GenericRecord](writer)
        fileWriter.create(schema,output)

        fileWriter.append(record)
        fileWriter.flush()
        fileWriter.close()

        output.toByteArray
    }
}
