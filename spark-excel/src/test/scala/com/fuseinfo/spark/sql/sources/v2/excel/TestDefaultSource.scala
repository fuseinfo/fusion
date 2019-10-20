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
package com.fuseinfo.spark.sql.sources.v2.excel

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.FunSuite

class TestDefaultSource extends FunSuite {
  var spark: SparkSession = _
  var sc: SparkContext = _
  implicit var sqlContext: SQLContext = _

    spark = SparkSession.builder
      .master("local")
      .appName("Spark session for testing")
      .getOrCreate()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext

  test("Read simple excel 97 file") {
    val df = spark.read
      .format("com.fuseinfo.spark.sql.sources.v2.excel")
      .option("header","true")
      .load("examples/xls/")
    df.createOrReplaceTempView("EXCEL_READER")
    assert(df.count === 150)
    assert(spark.sql("SELECT SUM(CAST(sepal_length AS DECIMAL(8,2))) FROM EXCEL_READER").collect()
      .head.getDecimal(0) === new java.math.BigDecimal("876.50"))
  }

  test("Read simple excel 2007 file") {
    val df = spark.read
      .format("com.fuseinfo.spark.sql.sources.v2.excel")
      .option("header","true")
      .load("examples/xlsx/")
    df.createOrReplaceTempView("EXCEL_READER2")
    assert(df.count === 150)
    assert(spark.sql("SELECT SUM(CAST(sepal_length AS DECIMAL(8,2))) FROM EXCEL_READER2").collect()
      .head.getDecimal(0) === new java.math.BigDecimal("876.50"))
  }
}
