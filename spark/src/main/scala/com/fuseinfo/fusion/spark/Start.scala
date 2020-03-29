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
package com.fuseinfo.fusion.spark

import com.fuseinfo.fusion.FusionFunction
import com.fuseinfo.fusion.util.{ClassUtils, VarUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.util.Try

class Start(taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val udfNames = collection.mutable.Set.empty[String]

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars:java.util.Map[String, String]): String = {
    val credentialPath = "hadoop.security.credential.provider.path"
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val jceks = enrichedParams.getOrElse(credentialPath, System.getProperty(credentialPath))
    if (jceks != null) {
     val conf = new Configuration
      conf.set(credentialPath, jceks)
      CredentialProviderFactory.getProviders(conf).foreach(provider => provider.getAliases.foreach(alias =>
      vars.put(alias, new String(provider.getCredentialEntry(alias).getCredential))))
    }

    val sparkConf = new SparkConf()
    enrichedParams.filter(_._1.startsWith("spark.")).foreach(kv => sparkConf.set(kv._1, kv._2))
    if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder.appName("Fusion-Spark").config(sparkConf).getOrCreate
    params.get("udf") match {
      case udfList:String =>
        udfList.split(",").foreach(udfPackage =>
          ClassUtils.getAllClasses(udfPackage.trim, null)
            .foreach(kv => registerUDF(spark, kv._1.toUpperCase, kv._2))
        )
      case _ =>
    }
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.sftp.impl", "org.apache.hadoop.fs.sftp.SftpFileSystem")
    enrichedParams.filter(_._1.startsWith("hadoop.")).foreach(kv => hadoopConf.set(kv._1.substring(7), kv._2))
    logger.info("Started Spark")
    "Started Spark"
  }

  override def close(): Unit = Try(FileSystem.closeAll())

  def registerUDF(spark:SparkSession, udfName:String, clazz:Class[_]): Unit = {
    if (!udfNames.contains(udfName)) {
      scala.util.Try{
        val dataType = if (classOf[UserDefinedAggregateFunction].isAssignableFrom(clazz)) {
          null
        } else if (classOf[WithReturnType].isAssignableFrom(clazz)) {
          clazz.newInstance().asInstanceOf[WithReturnType].returnType
        } else {
          val methods = clazz.getDeclaredMethods
          val rtClass = methods(methods.indexWhere(m =>
            "call" == m.getName && "java.lang.Object" != m.getReturnType.getName)).getReturnType
          val realClass = if (rtClass.isArray) rtClass.getComponentType else rtClass
          val realClassName = realClass.getName
          val realType = realClassName match {
            case "boolean" | "java.lang.Boolean" => DataTypes.BooleanType
            case "byte" | "java.lang.Byte" => DataTypes.ByteType
            case "java.sql.Date" => DataTypes.DateType
            case "double" | "java.lang.Double" => DataTypes.DoubleType
            case "float" | "java.lang.Float" => DataTypes.FloatType
            case "int" | "java.lang.Integer" => DataTypes.IntegerType
            case "long" | "java.lang.Long" => DataTypes.LongType
            case "short" | "java.lang.Short" => DataTypes.ShortType
            case "java.lang.String" => DataTypes.StringType
            case "java.sql.Timestamp" => DataTypes.TimestampType
            case _ => DataTypes.NullType
          }
          if (rtClass.isArray) {
            if (realType == DataTypes.ByteType) DataTypes.BinaryType
            else ArrayType(realType)
          } else realType
        }
        clazz.newInstance match {
          case udf:UserDefinedAggregateFunction => spark.udf.register(udfName, udf)
          case udf:UDF1[_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF2[_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF3[_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF4[_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF5[_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF6[_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF7[_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF8[_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF9[_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF10[_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF11[_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF12[_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF13[_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF14[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF15[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF16[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF17[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF18[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF19[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF20[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF21[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case udf:UDF22[_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_] => spark.udf.register(udfName, udf, dataType)
          case _ =>
        }
      }
    }
  }

  override def getProcessorSchema:String = """{"title": "Start","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.Start"}
    },"required":["__class"]}"""
}

trait WithReturnType {
  def returnType:DataType
}