/*
 * Copyright 2018 Fuseinfo Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fuseinfo.fusion.spark

import com.fuseinfo.fusion.spark.reader.KafkaReader
import com.fuseinfo.fusion.spark.writer.{DataWriterBase, KafkaWriter}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.FunSuite

class KafkaReaderWriterSuite extends FunSuite with DataWriterBase with EmbeddedKafka{
  test("test kafka reader writer") {
    if (!System.getProperty ("os.name").toLowerCase.startsWith("windows")) {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
        customBrokerProperties=Map("zookeeper.connection.timeout.ms"->"60000"))
      withRunningKafka{
        scala.util.Try(EmbeddedKafka.createCustomTopic("unit-test"))
        val params = new java.util.HashMap[String, AnyRef]
        params.put("topic", "unit-test")
        params.put("table", "DATA_WRITER")
        params.put("kafka.bootstrap.servers", "localhost:6001")
        val writer = new KafkaWriter("KAFKA_WRITER", params)
        writer(vars)
        params.clear()
        params.put("topic", "unit-test")
        params.put("kafka.enable.auto.commit", "false")
        params.put("kafka.bootstrap.servers", "localhost:6001")
        params.put("kafka.group.id", "unit-test-cg1")
        params.put("schema", """{"type":"record","name":"DATA_WRITER","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"dob","type":["null",{"type":"int","logicalType":"date"}],"default":null},{"name":"amt","type":["null",{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}],"default":null},{"name":"lud","type":["null",{"type":"long","logicalType":"timestamp-micros"}],"default":null}]}""")
        val reader = new KafkaReader("KAFKA_READER", params)
        reader(vars)
        val dfOut = spark.table("KAFKA_READER")
        assert(dfOut.count === 2)
        val out = dfOut.collect.map(_.toString).sorted
        assert(out(0).startsWith("[1,foo,1970-01-01,12.34,2018-01-0"))
        assert(out(1).startsWith("[2,bar,1980-02-02,56.78,2018-02-0"))
      }
    }
  }
}
