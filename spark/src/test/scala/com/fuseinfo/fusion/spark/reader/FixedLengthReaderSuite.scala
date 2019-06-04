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

class FixedLengthReaderSuite extends FunSuite with SparkBase {

  test("Read flat file"){
    val params = new java.util.HashMap[String, AnyRef]
    params.put("path", "examples/txt")
    params.put("fields", "sepal_length:0,sepal_width:3,petal_length:6,petal_width:9,species:12")
    val reader = new FixedLengthReader("FIXED_READER", params)
    reader(vars)
    val fixedDF = spark.table("FIXED_READER")
    assert(fixedDF.count === 150)
    assert(spark.sql("SELECT SUM(CAST(sepal_length AS DECIMAL(8,2))) FROM FIXED_READER").collect()
      .head.getDecimal(0) === new java.math.BigDecimal("876.50"))
  }
}
