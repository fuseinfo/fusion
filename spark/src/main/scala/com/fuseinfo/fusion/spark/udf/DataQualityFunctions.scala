package com.fuseinfo.fusion.spark.udf

import org.apache.spark.sql.api.java.{UDF2, UDF3}

import java.util.regex.Pattern
import scala.collection.mutable

class IsComplete extends UDF2[AnyRef, String, String]{
  override def call(value: AnyRef, message: String): String =
    if (value == null || String.valueOf(value).isEmpty) message else ""
}

class HasMinLength extends UDF3[String, String, Int, String] {
  override def call(value: String, message: String, length: Int): String =
    if (value == null || value.length < length) message else ""
}

class HasMaxLength extends UDF3[String, String, Int, String] {
  override def call(value: String, message: String, length: Int): String =
    if (value == null || value.length > length) message else ""
}

class HasPattern extends UDF3[String, String, String, String] {
  private val buffer = mutable.Map.empty[String, Pattern]

  override def call(value: String, message: String, pattern: String): String = {
    val regex = buffer.getOrElseUpdate(pattern, Pattern.compile(pattern))

    if (value == null || !regex.matcher(value).matches()) message else ""
  }
}