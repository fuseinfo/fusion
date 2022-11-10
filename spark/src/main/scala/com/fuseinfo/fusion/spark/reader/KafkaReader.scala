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
package com.fuseinfo.fusion.spark.reader

import java.util

import com.fuseinfo.fusion.spark.util.{AvroUtils, SparkUtils}
import com.fuseinfo.fusion.util.VarUtils
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class KafkaReader(taskName:String, params:util.Map[String, AnyRef])
  extends (util.Map[String, String] => String) with Serializable {

  def this(taskName:String) = this(taskName, new util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(vars:util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val kafkaParams = new util.HashMap[String, AnyRef]
    enrichedParams.filter(_._1.startsWith("kafka.")).foreach(kv => kafkaParams.put(kv._1.substring(6), kv._2))
    kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaParams.put("enable.auto.commit", "false")
    kafkaParams.put("auto.offset.reset", "earliest")
    val topic = enrichedParams("topic")
    val subject = topic + "-value"
    val schemaStr = params.get("schema")
    val schemaRegistryUrl = kafkaParams.get("schema.registry.url")
    val valueSchema = schemaRegistryUrl match {
      case url:String if schemaStr == null =>
        val schemaRegistry = new CachedSchemaRegistryClient(url, 1000)
        schemaRegistry.getLatestSchemaMetadata(subject).getSchema
      case _ => params.get("schema").toString
    }
    val avroSchema = (new Schema.Parser).parse(valueSchema)
    val schema = AvroUtils.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
    val offsetRanges = enrichedParams.get("ranges") match {
      case Some(ranges) =>
        ranges.split(";").map{range =>
          val bgnIdx = range.indexOf(':')
          val endIdx = range.indexOf('-', bgnIdx)
          OffsetRange.create(topic, range.substring(0, bgnIdx).toInt,
            range.substring(bgnIdx + 1, endIdx).toLong, range.substring(endIdx + 1).toLong)
        }
      case None =>
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
        consumer.subscribe(util.Arrays.asList(topic))
        consumer.poll(0)
        val topicParts = consumer.assignment
        val beginMap = topicParts.map(tp => tp.partition -> consumer.position(tp)).toMap
        consumer.seekToEnd(topicParts)
        val endMap = topicParts.map(tp => tp.partition -> consumer.position(tp)).toMap
        val maxSize = enrichedParams.getOrElse("maxSize", "2147483647").toInt
        enrichedParams.get("offsets") match {
          case Some(offsets) =>
            val ranges = offsets.split(";").map{offset =>
              val idx = offset.indexOf(':')
              val part = offset.substring(0, idx).toInt
              val bgn = offset.substring(idx + 1).toLong
              OffsetRange.create(topic, part, bgn, Math.min(bgn + maxSize, endMap.getOrElse(part, bgn)))
            }
            consumer.close()
            ranges
          case None =>
            val ranges = topicParts.map{tp =>
              val part = tp.partition
              val bgn = beginMap(part)
              OffsetRange.create(topic, part, bgn, Math.min(bgn + maxSize, endMap(part)))
            }.toArray
            consumer.close()
            ranges
        }
    }
    vars.put(taskName + "_KAFKA_RANGES",
      offsetRanges.map(range => range.partition + ":" + range.fromOffset + "-" + range.untilOffset).mkString(";"))
    logger.info("{} reading from topic {}", taskName, topic:Any)
    offsetRanges.foreach(range => logger.info(range.toString))
    val rdd = KafkaUtils.createRDD[Array[Byte], Array[Byte]](spark.sparkContext, kafkaParams,
      offsetRanges, LocationStrategies.PreferConsistent)
    val rowRDD = rdd.mapPartitions{iter =>
      val converterMap = collection.mutable.Map.empty[Int, (AnyRef=>AnyRef, GenericDatumReader[GenericRecord])]
      val (schemaRegistry, defaultPair) = schemaRegistryUrl match {
        case url:String => (new CachedSchemaRegistryClient(url, 1000), null)
        case _ =>
          val avroSchema = (new Schema.Parser).parse(valueSchema)
          val rowConverter = AvroUtils.createConverterToSQL(avroSchema, schema)
          val rowReader = new GenericDatumReader[GenericRecord](avroSchema)
          (null, (rowConverter, rowReader))
      }
      val decoderFactory = DecoderFactory.get
      iter.map{record =>
        val buffer = record.value
        val (converter, data) = if (schemaRegistry != null) {
          val id = buffer(1) << 24 | (buffer(2) & 0xFF) << 16 | (buffer(3) & 0xFF) << 8 | (buffer(4) & 0xFF)
          val converter = converterMap.get(id) match {
            case Some(func) => func
            case None =>
              val avroSchema = schemaRegistry.getBySubjectAndId(subject, id)
              val rowConverter = AvroUtils.createConverterToSQL(avroSchema, schema)
              val rowReader = new GenericDatumReader[GenericRecord](avroSchema)
              val converterPair = (rowConverter, rowReader)
              converterMap.put(id, converterPair)
              converterPair
          }
          val data = new Array[Byte](buffer.length - 5)
          System.arraycopy(buffer, 5, data, 0, data.length)
          (converter, data)
        } else {
          (defaultPair, buffer)
        }

        val row = converter._1(converter._2.read(null, decoderFactory.binaryDecoder(data, null)))
        Row(row.asInstanceOf[GenericRow].toSeq:_*)
      }
    }
    val df = spark.createDataFrame(rowRDD, schema)
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Read Kafka data from $topic lazily"
  }

  def getProcessorSchema:String = """{"title": "KafkaReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.KafkaReader"},
    "topic":{"type":"string","description":"Kafka topic"},
    "kafka.bootstrap.servers":{"type":"string","description":"Kafka bootstrap servers"},
    "kafka.group.id":{"type":"string","description":"Group ID"},
    "kafka.schema.registry.url":{"type":"string","description":"Schema registry url"},
    "schema":{"type":"string","description":"Schema"},
    "ranges":{"type":"string","description":"List of ranges"},
    "offsets":{"type":"string","description":"List of offsets"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "cache":{"type":"boolean","description":"cache the DataFrame?"},
    "viewName":{"type":"string","description":"View Name to be registered"}
    },"required":["__class","topic","kafka.bootstrap.servers","kafka.group.id"]}"""
}
