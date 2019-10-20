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
package com.fuseinfo.spark.sql.sources.v2.cobol

import java.io.ByteArrayInputStream

import collection.JavaConversions._
import com.fuseinfo.spark.utils.{ByteBufferInputStream, FilePart}
import net.sf.JRecord.Common.FieldDetail
import net.sf.JRecord.Details.LayoutDetail
import net.sf.JRecord.External.CobolCopybookLoader
import net.sf.JRecord.IO.LineIOProvider
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FixedLengthInputFormat}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import scala.util.Try

class CobolReaderFactory(val part:FilePart, val conf:Configuration, copyBook:Array[Byte], bookName:String,
                         split:Int, font:String, copyBookFormat:Int, binaryFormat:Int, recordFormat:String,
                         numberType:Int, isTree:Boolean, nullValue:String, emptyValue:String)
  extends DataReaderFactory[Row] {

  def createDataReader(): DataReader[Row] = {
    val layoutDetail = (new CobolCopybookLoader).loadCopyBook(new ByteArrayInputStream(copyBook), bookName,
      split,0, font, copyBookFormat, binaryFormat, 0, null).asLayoutDetail
    val schema = CobolDataSourceReader.createSchema(isTree, layoutDetail, numberType, copyBook, bookName,
      copyBookFormat)
    new CobolDataReader(layoutDetail, new FileSplit(new Path(part.file), part.start, part.length, part.hosts),
      schema, conf, recordFormat, numberType, isTree, nullValue, emptyValue)
  }
}

class CobolDataReader(layoutDetail:LayoutDetail, split:InputSplit, schema:StructType, conf:Configuration,
                      recordFormat:String, numberType:Int, isTree:Boolean, nullValue:String, emptyValue:String)
  extends DataReader[Row] {

  private val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
  private val inputFormat =
    if (recordFormat.startsWith("F")) new FixedLengthInputFormat else new VariableLengthInputFormat
  private val recordReader = inputFormat.createRecordReader(split, context)
  recordReader.initialize(split, context)
  private val bbis = new ByteBufferInputStream
  private val reader = LineIOProvider.getInstance.getLineReader(layoutDetail, null)

  override def next(): Boolean = recordReader.nextKeyValue()

  override def get(): Row = {
    bbis.setData(recordReader.getCurrentValue.getBytes)
    reader.open(bbis, layoutDetail)
    val line = reader.read()
    val iter = line.getFieldIterator(0)
    val buffer = collection.mutable.Map.empty[String, Any]
    while (iter.hasNext) {
      val vv = iter.next()
      val f = vv.getFieldDetail
      val name = f.getName
      val stdname = CobolDataSourceReader.stdField(name)
      val bname = if (isTree) CobolDataSourceReader.stdGroup(f.asInstanceOf[FieldDetail].getGroupName) + stdname
                  else stdname
      val data = f.getType match {
        case 22|23|24|25|26|27|28|29|32 =>
          val dec = f.getDecimal
          val len = f.getLen
          if (dec > 0 || len > 18 || numberType == CobolDataSourceReader.AS_DECIMAL)
            Try(if (numberType == CobolDataSourceReader.AS_STRING) checkString(vv.asString)
                else if (numberType == CobolDataSourceReader.AS_DOUBLE) vv.asDouble
                else vv.asBigDecimal()).getOrElse(null)
          else if (len > 9) Try(vv.asLong).getOrElse(null)
          else Try(vv.asInt).getOrElse(null)
        case 35|36|37|38|39|142|143|144 =>
          val dec = f.getDecimal
          val len = f.getLen
          if (dec > 0 || len > 7 || numberType == CobolDataSourceReader.AS_DECIMAL)
            Try(if (numberType == CobolDataSourceReader.AS_STRING) checkString(vv.asString)
            else if (numberType == CobolDataSourceReader.AS_DOUBLE) vv.asDouble
            else vv.asBigDecimal()).getOrElse(null)
          else if (len > 3) Try(vv.asLong).getOrElse(null)
          else Try(vv.asInt).getOrElse(null)
        case 31|33|140|141 =>
          val dec = f.getDecimal
          val len = f.getLen << 1
          if (dec > 0 || len > 18 || numberType == CobolDataSourceReader.AS_DECIMAL)
            Try(if (numberType == CobolDataSourceReader.AS_STRING) checkString(vv.asString)
                else if (numberType == CobolDataSourceReader.AS_DOUBLE) vv.asDouble
                else vv.asBigDecimal()).getOrElse(null)
          else if (len > 9) Try(vv.asLong).getOrElse(null)
          else Try(vv.asInt).getOrElse(null)
        case _ => checkString(vv.asString)
      }
      val matcher = CobolDataSourceReader.occursRegex.matcher(name)
      if (matcher.matches) {
        val fname = CobolDataSourceReader.stdField(matcher.group(1))
        var parent = buffer.getOrElseUpdate(bname,
          collection.mutable.ArrayBuffer.empty[Any]).asInstanceOf[collection.mutable.ArrayBuffer[Any]]
        val gp = matcher.group(2)
        var beginIdx = 0
        var endIdx = gp.indexOf(',')
        while (endIdx > beginIdx) {
          val idx = gp.substring(beginIdx, endIdx).trim.toInt
          if (parent.size > idx) parent = parent(idx).asInstanceOf[collection.mutable.ArrayBuffer[Any]]
          else {
            val next = collection.mutable.ArrayBuffer.empty[Any]
            parent.append(next)
            parent = next
          }
          beginIdx = endIdx + 1
          endIdx = gp.indexOf(',', beginIdx)
        }
        parent.append(data)
      } else buffer.put(bname, data)
    }
    createRow(buffer, schema.fields)
  }

  override def close(): Unit = {}

  private def createRow(buffer:collection.mutable.Map[String, Any], fields: Array[StructField],
                groupName: String = ".", levels:List[Int] = Nil): Row = {
    Try{
      Row(fields.map{field =>
        val fname = field.name
        val bname = if (isTree) groupName + fname else fname
        field.dataType match {
          case array: ArrayType => createArray(buffer, array, fname, groupName, levels)
          case record: StructType => createRow(buffer, record.fields, groupName + fname + ".", levels)
          case _ =>
            buffer.get(bname) match {
              case Some(data) => if (levels.isEmpty) data
                  else levels.foldRight(data)((i, op) => op.asInstanceOf[collection.mutable.ArrayBuffer[Any]].get(i))
              case None => null
            }
        }
      }:_*)
    }.getOrElse(null)
  }

  private def createArray(buffer:collection.mutable.Map[String, Any], array: ArrayType,
                  fname:String, groupName:String, levels:List[Int]): Array[Any] = {
    Try{
      val bname = if (isTree) groupName + fname else fname
      val fvalue = buffer.get(bname) match {
        case Some(data) => levels.foldRight(data)((i,p) =>
          p.asInstanceOf[collection.mutable.ArrayBuffer[Any]].get(i)).asInstanceOf[collection.mutable.ArrayBuffer[Any]]
        case None => null
      }
      val result = Stream.from(0).map{i =>
        array.elementType match {
          case record: StructType => createRow(buffer, record.fields, groupName, i::levels)
          case child: ArrayType => createArray(buffer, array, fname, groupName, i::levels)
          case _ => if (fvalue != null && i < fvalue.size) fvalue(i) else null
        }
      }.takeWhile(_ != null).toArray
      if (result.length == 0) null else result
    }.getOrElse(null)
  }

  protected def isValidX(str:String): Boolean = str.length == 0 || str.charAt(0) != 0

  protected def checkString(s: String): String =
    if (nullValue != null && nullValue == s.trim) emptyValue else if (isValidX(s)) s else emptyValue
}