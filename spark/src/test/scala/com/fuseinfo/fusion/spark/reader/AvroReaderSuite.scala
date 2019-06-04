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
package com.fuseinfo.fusion.spark.reader

import com.fuseinfo.fusion.spark.SparkBase
import org.scalactic.TolerantNumerics
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class AvroReaderSuite extends FunSuite with SparkBase {

  test("Read Avro file"){
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)
    val reader = new AvroReader("AVRO_READER", Map[String, AnyRef]("path" -> "examples/avro").asJava)
    reader(vars)
    val avroDF = spark.table("AVRO_READER")
    assert(avroDF.count === 150)
    assert(spark.sql("SELECT SUM(sepal_length) FROM AVRO_READER").collect().head.getDouble(0) === 876.5)
  }
}

