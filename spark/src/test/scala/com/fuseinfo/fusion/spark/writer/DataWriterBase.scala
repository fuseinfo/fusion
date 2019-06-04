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
package com.fuseinfo.fusion.spark.writer

import java.sql.{Date, Timestamp}

import com.fuseinfo.fusion.spark.SparkBase
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types._

trait DataWriterBase extends SparkBase {
  val df:Dataset[Row] = {
    val schema = StructType(Seq(StructField("id", LongType), StructField("name", StringType),
      StructField("dob", DateType), StructField("amt", DecimalType(12,2)), StructField("lud", TimestampType)))
    val data = Array(
      Row(1L, "foo", new Date(70, 0, 1), new java.math.BigDecimal(12.34), new Timestamp(1514851200000L)),
      Row(2L, "bar", new Date(80, 1, 2), new java.math.BigDecimal(56.78), new Timestamp(1517565600000L)))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }
  df.createOrReplaceTempView("DATA_WRITER")
}
