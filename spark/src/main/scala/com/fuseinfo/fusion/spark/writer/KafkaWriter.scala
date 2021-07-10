/*
 * Copyright (c) 2018 Fuseinfo Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.fuseinfo.fusion.spark.writer

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util

import com.fuseinfo.fusion.spark.util.AvroUtils
import com.fuseinfo.fusion.util.VarUtils
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class KafkaWriter(taskName:String, params:java.util.Map[String, AnyRef])
  extends (util.Map[String, String] => String) with Serializable {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(vars:java.util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val topic = enrichedParams("topic")
    val kafkaProps = enrichedParams.filter(_._1.startsWith("kafka.")).map(kv => kv._1.substring(6) -> kv._2) ++
      Map("key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
        "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer")
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val schemaRegistryUrl = kafkaProps.get("schema.registry.url")
    val schemaRegistry = schemaRegistryUrl match {
      case Some(url) => new CachedSchemaRegistryClient(url, 1000)
      case None => null
    }
    val tableName = enrichedParams.getOrElse("table", params("__previous")).toString
    val df = spark.sqlContext.table(tableName)
    val df2 = enrichedParams.get("coalesce") match {
      case Some(num) => df.coalesce(num.toInt)
      case None =>
        enrichedParams.get("repartition") match {
          case Some(num) => df.repartition(num.toInt)
          case None => df
        }
    }

    val keySchemaOpt = enrichedParams.get("keyColumn")
      .map( keyColumn => AvroUtils.toAvroSchema(tableName + "_key", df.select(keyColumn).schema.fields))
    val keyId = if (schemaRegistry != null) keySchemaOpt.map{keySchema =>
      val id = schemaRegistry.register(topic + "-key", keySchema)
      ByteBuffer.allocate(4).putInt(id).array}
    else None
    val keySchemaStrOpt = keySchemaOpt.map(keySchema => keySchema.toString)
    val fields = df.schema.fields
    val fieldNames = fields.map(f => f.name)
    val valueSchema = AvroUtils.toAvroSchema(tableName, fields)
    val valueSchemaStr = valueSchema.toString()
    val valueId = if (schemaRegistry != null)
      ByteBuffer.allocate(4).putInt(schemaRegistry.register(topic + "-value", valueSchema)).array
    else Array.emptyByteArray
    logger.info("{} Writing data to topic {}", taskName, topic:Any)
    df2.foreachPartition{iter: Iterator[Row] =>
      val parser = new Schema.Parser
      val valueSchema = parser.parse(valueSchemaStr)
      val keySchemaOpt = keySchemaStrOpt.map(schemaStr => parser.parse(schemaStr))
      var error:Exception = null
      val stream = new ByteArrayOutputStream
      val keyWriter = keySchemaOpt.map(schemaStr => new GenericDatumWriter[GenericRecord](schemaStr))
      val valueWriter = new GenericDatumWriter[GenericRecord](valueSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](kafkaProps)
      iter.foreach{row =>
        val (avroKey, avroValue) = rowToAvro(keySchemaOpt, valueSchema, row, fieldNames)
        val keyBytes = if (avroKey != null && keySchemaOpt.nonEmpty) {
          stream.reset()
          keyId.foreach{id =>
            stream.write(0)
            stream.write(id)
          }
          keyWriter.get.write(avroKey, encoder)
          encoder.flush()
          stream.toByteArray
        } else null
        stream.reset()
        if (valueId.nonEmpty) {
          stream.write(0)
          stream.write(valueId)
        }
        valueWriter.write(avroValue, encoder)
        encoder.flush()
        val valueBytes = stream.toByteArray
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, keyBytes, valueBytes)
        producer.send(record, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (error == null && exception != null) error = exception
          }
        })
      }
      if (error != null) throw error
      producer.flush()
      if (error != null) throw error
    }
    s"Persisted data to topic $topic"
  }

  private def sqlValueToAvro(value: Any, fieldName: String): Any = {
    value match {
      case null | _:java.lang.Boolean | _:java.lang.Integer | _:java.lang.Long | _:java.lang.Float |
           _:java.lang.Double | _:String => value
      case v: java.lang.Byte => v.toInt
      case v: java.lang.Short => v.toInt
      case v: java.math.BigDecimal => ByteBuffer.wrap(v.unscaledValue.toByteArray)
      case v: Array[Byte] => ByteBuffer.wrap(v)
      case v: java.sql.Date => v.toLocalDate.toEpochDay.toInt
      case v: java.sql.Timestamp => AvroUtils.toEpochMicroSecs(v)
      case v: collection.Seq[Any] => v.map(sqlValueToAvro(_, fieldName)).asJavaCollection
      case v: collection.Map[_, _] =>
        val javaMap = new java.util.HashMap[String, Any]
        v.foreach(kv => javaMap.put(String.valueOf(kv._1), sqlValueToAvro(kv._2, fieldName)))
        javaMap
      case v: Row => rowToAvroData(v, AvroUtils.toAvroSchema(fieldName, v.schema.fields))
    }
  }

  private def rowToAvroData(row: Row, schema:Schema): GenericRecord = {
    val record = new GenericData.Record(schema)
    val fields = schema.getFields.map(_.name).toArray
    val size = row.size
    for (i <- 0 until size) record.put(i, sqlValueToAvro(row.get(i), fields(i)))
    record
  }

  private def rowToAvro(keySchemaOpt:Option[Schema], valueSchema:Schema, row:Row, fieldNames:Array[String]) = {
    val keyRecord = keySchemaOpt match {
      case Some(keySchema) => new GenericData.Record(keySchema)
      case None => null
    }
    val valRecord = new GenericData.Record(valueSchema)
    for (i <- 0 until row.size) {
      val keyField = keySchemaOpt match {
        case Some(keySchema) => keySchema.getField(fieldNames(i))
        case None => null
      }
      val valField = valueSchema.getField(fieldNames(i))
      if (valField != null || keyField != null) {
        val value = sqlValueToAvro(row.get(i), fieldNames(i))
        if (valField != null) valRecord.put(valField.pos, value)
        if (keyField != null) keyRecord.put(keyField.pos, value)
      }
    }
    (keyRecord, valRecord)
  }

  def getProcessorSchema:String = """{"title": "KafkaWriter","type":"object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.writer.KafkaWriter"},
    "topic":{"type":"string","description":"Kafka topic"},
    "kafka.bootstrap.servers":{"type":"string","description":"Kafka bootstrap servers"},
    "kafka.schema.registry.url":{"type":"string","description":"Kafka schema registry url"},
    "sql":{"type":"string","format":"sql","description":"Spark SQL statement",
      "options":{"ace":{"useSoftTabs":true,"maxLines":16}}},
    "table":{"type":"string","description":"Table name"},
    "keyColumn":{"type":"string","description":"Key column"},
    "coalesce":{"type":"string","format":"number","description":"Number of partitions to coalesce"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "onSuccess":{"type":"array","format":"tabs","description":"extension after success",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}},
    "onFailure":{"type":"array","format":"tabs","description":"extension after failure",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}}
    },"required":["__class","topic","kafka.bootstrap.servers"]}"""
}
