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
import org.scalactic.{Equality, TolerantNumerics}

class CsvReaderSuite extends FunSuite with SparkBase {

  test("Read simple CSV file"){
    implicit val doubleEquality:Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.01)
    val params = new java.util.HashMap[String, AnyRef]
    params.put("path", "examples/csv")
    params.put("header", "true")
    params.put("inferSchema", "true")
    params.put("columnNameOfCorruptRecord", "_corrupt_record")
    val reader = new CsvReader("CSV_READER", params)
    reader(vars)
    val csvDF = spark.table("CSV_READER")
    assert(csvDF.count === 150)
    assert(spark.sql("SELECT SUM(sepal_length) FROM CSV_READER").collect().head.getDouble(0) === 876.5)
  }

  test("Read CSV with extra lines"){
    val params = new java.util.HashMap[String, AnyRef]
    params.put("path", "examples/extra_header_csv")
    params.put("header", "true")
    params.put("skipLines", "1")
    params.put("inferSchema", "true")
    val reader = new CsvReader("CSV_READER2", params)
    reader(vars)
    val csvDF2 = spark.table("CSV_READER2")
    assert(csvDF2.count === 3)
    val results = csvDF2.collect().sortBy(_.getString(0))
    assert(results(0).toString == "[Iris-setosa,bristle-pointed iris,Asia, North America]")
    assert(results(1).toString == "[Iris-versicolor,blue flag,North America]")
    assert(results(2).toString == "[Iris-virginica,Virginia iris,Eastern North America]")
    val fields = csvDF2.schema.fields
    assert(fields(0).name == "species")
    assert(fields(1).name == "alias")
    assert(fields(2).name == "distribution")
  }
}
