package com.fuseinfo.fusion.ext

import com.fuseinfo.fusion.util.VarUtils
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class KafkaCommit(params: Map[String, String]) extends (java.util.Map[String, String] => Boolean) {
  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(stats: java.util.Map[String, String]) : Boolean = try {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v, stats))
    val kafkaParams = new java.util.HashMap[String, AnyRef]
    enrichedParams.filter(_._1.startsWith("kafka.")).foreach(kv => kafkaParams.put(kv._1.substring(6), kv._2))
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaParams.put("enable.auto.commit", "false")
    kafkaParams.put("auto.offset.reset", "earliest")
    val offsetString = enrichedParams("offsets")
    val topic = enrichedParams("topic")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
    val offsets = offsetString.split(";").map{os =>
      val idx = os.indexOf(':')
      val dash = os.indexOf('-', idx)
      new TopicPartition(topic, os.substring(0, idx).toInt) -> new OffsetAndMetadata(os.substring(dash + 1).toLong)
    }.toMap.asJava
    consumer.commitSync(offsets)
    logger.info(s"KafkaCommit extension successful to $topic: $offsetString")
    consumer.close()
    true
  } catch {
    case e:Exception =>
      logger.warn("Failed to run KafkaCommit extension", e)
      false
  }
}
