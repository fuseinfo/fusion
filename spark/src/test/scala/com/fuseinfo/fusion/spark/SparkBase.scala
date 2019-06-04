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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkBase extends FusionBase{
  val spark:SparkSession = {
    val sparkConf = new SparkConf
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.sql.parquet.writeLegacyFormat", "true")
    SparkSession.builder.appName("FUSION-TEST").config(sparkConf).getOrCreate
  }
}
