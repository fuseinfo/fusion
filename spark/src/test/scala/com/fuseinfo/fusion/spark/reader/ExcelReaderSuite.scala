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
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class ExcelReaderSuite extends FunSuite with SparkBase {

  test("Read simple excel 97 file") {
    val reader = new ExcelReader("EXCEL_READER",
      Map[String, AnyRef]("path" -> "examples/xls", "useHeader" -> "true").asJava)
    reader(vars)
    val fixedDF = spark.table("EXCEL_READER")
    assert(fixedDF.count === 150)
    assert(spark.sql("SELECT SUM(CAST(sepal_length AS DECIMAL(8,2))) FROM EXCEL_READER").collect()
      .head.getDecimal(0) === new java.math.BigDecimal("876.50"))
  }

  test("Read simple excel 2007 file") {
    val reader = new ExcelReader("EXCEL_READER2",
      Map[String, AnyRef]("path" -> "examples/xlsx", "useHeader" -> "true").asJava)
    reader(vars)
    val fixedDF = spark.table("EXCEL_READER2")
    assert(fixedDF.count === 150)
    assert(spark.sql("SELECT SUM(CAST(sepal_length AS DECIMAL(8,2))) FROM EXCEL_READER2").collect()
      .head.getDecimal(0) === new java.math.BigDecimal("876.50"))
  }
}
