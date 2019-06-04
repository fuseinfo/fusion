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

import java.math.BigInteger
import java.sql.Timestamp
import java.time.LocalDate
import java.util.concurrent.TimeUnit

import com.google.common.primitives.{Ints, Longs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.ColumnIOFactory
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class ParquetWriterSuite extends FunSuite with DataWriterBase {

  test("Write to parquet file"){
    val outputDir = "./tmp/ParquetWriter/output"
    val stagingDir = "./tmp/ParquetWriter/staging"
    val outputPath = new Path(outputDir)
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    scala.util.Try(fs.delete(new Path(outputDir), true))
    scala.util.Try(fs.delete(new Path(stagingDir), true))

    val writer = new ParquetWriter("PARQUET_WRITER",
      Map[String, AnyRef]("path" -> outputDir, "table"->"DATA_WRITER", "staging" -> stagingDir, "coalesce"->"1").asJava)
    writer(vars)
    val parquetFiles = fs.listStatus(outputPath)
    assert(parquetFiles.size === 1)
    val out = getParquetRecords(parquetFiles.head.getPath, conf){group =>
      val groupType = group.getType.getFields.get(3)
      val meta = groupType.asPrimitiveType().getDecimalMetadata
      val amt = new java.math.BigDecimal(new BigInteger(group.getBinary(3, 0).getBytes), meta.getScale)
      (group.getLong(0, 0), group.getString(1, 0), LocalDate.ofEpochDay(group.getInteger(2, 0)),
      amt, readInt96(group.getInt96(4, 0).getBytes))
    }.sortBy(_._1)
    assert(out(0)._1 === 1)
    assert(out(0)._2.toString === "foo")
    assert(out(0)._3.toString == "1970-01-01")
    assert(out(0)._4 === new java.math.BigDecimal("12.34"))
    assert(out(0)._5.toString.startsWith("2018-01-0"))
    assert(out(1)._1 === 2)
    assert(out(1)._2.toString === "bar")
    assert(out(1)._3.toString == "1980-02-02")
    assert(out(1)._4 === new java.math.BigDecimal("56.78"))
    assert(out(1)._5.toString.startsWith("2018-02-0"))
  }

  private def getParquetRecords[T](path:Path, conf:Configuration)(rt: Group => T) = {
    val readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER)
    val schema = readFooter.getFileMetaData.getSchema
    val r = new ParquetFileReader(conf, path, readFooter)
    Stream.continually(r.readNextRowGroup).takeWhile(_ != null).flatMap{pages =>
      val columnIO = new ColumnIOFactory().getColumnIO(schema)
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      1L to pages.getRowCount map (_ => rt(recordReader.read))
    }.toList
  }

  private def readInt96(bytes: Array[Byte]) = {
    val nanos = Longs.fromBytes(bytes(7), bytes(6), bytes(5), bytes(4), bytes(3), bytes(2), bytes(1), bytes(0))
    val days = Ints.fromBytes(bytes(11), bytes(10), bytes(9), bytes(8))
    val ts = new Timestamp((days - 2440588) * TimeUnit.DAYS.toMillis(1) + (nanos / 100000000L * 1000))
    ts.setNanos((nanos % 1000000000L).toInt)
    ts
  }
}
