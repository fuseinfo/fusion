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
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
import java.util.regex.Pattern

import com.fuseinfo.spark.utils.{InputPart, SerializableConfiguration}
import net.sf.JRecord.Common.FieldDetail
import net.sf.JRecord.Details.LayoutDetail
import net.sf.JRecord.External.CobolCopybookLoader
import net.sf.JRecord.Numeric.ICopybookDialects
import net.sf.JRecord.Option.ICobolSplitOptions
import net.sf.cb2xml.Cb2Xml2
import net.sf.cb2xml.`def`.Cb2xmlConstants
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit, FixedLengthInputFormat}
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.w3c.dom.{Node, NodeList}

import scala.collection.JavaConversions._

class DefaultSource extends DataSourceV2 with ReadSupport {
  def createReader(dso: DataSourceOptions): DataSourceReader = {
    new CobolDataSourceReader(dso)
  }
}

object CobolDataSourceReader {
  val AS_DEFAULT = 0
  val AS_DECIMAL = 1
  val AS_DOUBLE = 2
  val AS_STRING = 3
  val array0Regex: Pattern = Pattern.compile("\\S+ \\([0, ]+\\)")
  val occursRegex: Pattern = Pattern.compile("(\\S+) \\((.+)\\)")

  private abstract class FldItem(val isArray: Boolean) {
    def getName:String
  }

  private case class SingleFld(field: StructField) extends FldItem(field.dataType.isInstanceOf[ArrayType]) {
    override def getName: String = field.name
  }

  private case class ComplexFld(override val isArray: Boolean, name: String, items: collection.mutable.Buffer[FldItem])
    extends FldItem(isArray) {
    override def getName: String = name
  }

  def stdField(fname: String): String = fname.replaceAll("-", "_")
    .replaceAll("\\W", "").toLowerCase

  def stdGroup(gname: String): String = gname.replaceAll("-","_").toLowerCase

  private def getDataType(f: FieldDetail, numberType:Int) = {
    f.getType match {
      case 22|23|24|25|26|27|28|29|32 =>
        val dec = f.getDecimal
        val len = f.getLen
        if (dec > 0 || len > 18 || numberType == AS_DECIMAL) {
          if (numberType == AS_STRING) DataTypes.StringType
          else if (numberType == AS_DOUBLE) DataTypes.DoubleType
          else DecimalType(len, dec)
        } else if (len > 9) DataTypes.LongType else DataTypes.IntegerType
      case 35|36|37|38|39|142|143|144 =>
        val dec = f.getDecimal
        val len = f.getLen
        if (dec > 0 || len > 7 || numberType == AS_DECIMAL) {
          if (numberType == AS_STRING) DataTypes.StringType
          else if (numberType == AS_DOUBLE) DataTypes.DoubleType
          else DecimalType((len * 2.6).toInt, dec)
        } else if (len > 3) DataTypes.LongType else DataTypes.IntegerType
      case 31|33|140|141 =>
        val dec = f.getDecimal
        val len = f.getLen << 1
        if (dec > 0 || len > 18 || numberType == AS_DECIMAL) {
          if (numberType == AS_STRING) DataTypes.StringType
          else if (numberType == AS_DOUBLE) DataTypes.DoubleType
          else DecimalType(len, dec)
        } else if (len > 9) DataTypes.LongType else DataTypes.IntegerType
      case _ => DataTypes.StringType
    }
  }

  def createSchema(isTree:Boolean, layoutDetail: LayoutDetail, numberType:Int, copyBook:Array[Byte],
                   bookName:String, copyBookFormat:Int): StructType = {
    if (isTree) createTreeLayout(layoutDetail, numberType, copyBook, bookName, copyBookFormat)
    else createFlatLayout(layoutDetail, numberType)
  }

  private def createFlatLayout(layoutDetail:LayoutDetail, numberType:Int) = {
    new StructType(layoutDetail.getRecord(0).getFields.filter(f =>
      f.getName.indexOf(' ') < 0 || CobolDataSourceReader.array0Regex.matcher(f.getName).matches())
      .map { f =>
        val pos = f.getName.indexOf(' ')
        val fname = if (pos > 0) f.getName.substring(0, pos) else f.getName
        val dtype = getDataType(f, numberType)
        StructField(CobolDataSourceReader.stdField(fname), if (pos > 0) ArrayType(dtype) else dtype)
      }.toArray)
  }

  private def createTreeLayout(layoutDetail: LayoutDetail, numberType:Int, copyBook:Array[Byte], bookName:String,
                               copyBookFormat:Int) = {
    val copybookDoc = Cb2Xml2.convertToXMLDOM(new ByteArrayInputStream(copyBook), bookName, false, copyBookFormat)
    val fillerCounter = new AtomicInteger(-1)
    val root = nodeListToBuffer(copybookDoc.getFirstChild.getChildNodes, fillerCounter)
    val completed = collection.mutable.Set.empty[String]
    layoutDetail.getRecord(0).getFields.foreach{f =>
      val pos = f.getName.indexOf(' ')
      val realName = if (pos > 0) f.getName.substring(0, pos) else f.getName
      if (pos < 0 || !completed.contains(realName)) {
        if (pos > 0) completed.add(realName)
        val fname = stdField(realName)
        val groupName = f.getGroupName
        val groups = groupName.split("\\.").filter(_.length > 0)
        var parent = root
        for (group <- groups) {
          val stdGroup = stdField(group)
          parent.find(fi => stdGroup == fi.getName) match {
            case Some(sf:ComplexFld) => parent = sf.items
            case _ =>
          }
        }
        val dtype = getDataType(f, numberType)
        val idx = parent.indexWhere(fi => fi.getName == fname)
        if (idx >= 0) {
          parent.set(idx, SingleFld(StructField(fname, if (parent(idx).isArray) ArrayType(dtype) else dtype)))
        }
      }
    }
    bufferToStructType(root)
  }

  private def nodeListToBuffer(nl: NodeList, fillerCounter:AtomicInteger):collection.mutable.Buffer[FldItem] = {
    (for (i <- 0 until nl.getLength
         if nl.item(i).getNodeType == Node.ELEMENT_NODE
         if nl.item(i).getAttributes.getNamedItem("level").getNodeValue < "78")
    yield {
      val child = nl.item(i)
      val attr = child.getAttributes
      val fname = stdField(attr.getNamedItem("name").getNodeValue)
      val name = if ("filler" == fname) {
        val counter = fillerCounter.incrementAndGet
        fname + "_" + counter
      } else fname
      (attr.getNamedItem("picture"), attr.getNamedItem("occurs")) match {
        case (null, null) => ComplexFld(false, fname, nodeListToBuffer(child.getChildNodes, fillerCounter))
        case (pic, null) => SingleFld(StructField(name, StringType))
        case (null, occurs) => ComplexFld(true, fname, nodeListToBuffer(child.getChildNodes, fillerCounter))
        case (pic, occurs)  => SingleFld(StructField(name, ArrayType(StringType)))
      }
    }).toBuffer
  }

  private def bufferToStructType(buf: collection.mutable.Buffer[FldItem]):StructType = {
    StructType(buf.map{
      case f: SingleFld => f.field
      case f: ComplexFld => if (f.isArray) StructField(f.name, ArrayType(bufferToStructType(f.items)))
                          else StructField(f.name, bufferToStructType(f.items))
    })
  }
}

class CobolDataSourceReader(dso: DataSourceOptions) extends DataSourceReader {
  private val path = dso.get("path").orElse("")
  private val copyBook = dso.get("copybook").orElseThrow(new Supplier[Exception](){def get =
    new IllegalArgumentException("missing copybook option")}).getBytes(StandardCharsets.UTF_8)
  private val copyBookFormat = dso.getInt("copybookformat", Cb2xmlConstants.USE_STANDARD_COLUMNS)
  private val binaryFormat = dso.getInt("binaryformat", ICopybookDialects.FMT_MAINFRAME)
  private val split = dso.getInt("split", ICobolSplitOptions.SPLIT_NONE)
  private val bookName = dso get "bookname" orElse ""
  private val font = dso get "font" orElse (if (binaryFormat == ICopybookDialects.FMT_MAINFRAME) "cp037" else "")
  private val layoutDetail = (new CobolCopybookLoader).loadCopyBook(new ByteArrayInputStream(copyBook),
    bookName, split, 0, font, copyBookFormat, binaryFormat, 0, null).asLayoutDetail
  private val recordFormat = dso get "recordformat" orElse "F"
  private val recordLength = layoutDetail.getMaximumRecordLength
  private val numberType = dso.get("number").orElse("").toLowerCase() match {
    case "decimal" => CobolDataSourceReader.AS_DECIMAL
    case "double" => CobolDataSourceReader.AS_DOUBLE
    case "string" => CobolDataSourceReader.AS_STRING
    case _ => CobolDataSourceReader.AS_DEFAULT
  }
  private val isTree = dso.getBoolean("tree", false)
  private val nullValue = dso.get("nullvalue").orElse(null)
  private val emptyValue = dso.get("emptyvalue").orElse("")

  private val schema =
    CobolDataSourceReader.createSchema(isTree, layoutDetail, numberType, copyBook, bookName, copyBookFormat)

  override def readSchema(): StructType = schema

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    val spark = SparkSession.builder.getOrCreate
    val conf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    FixedLengthInputFormat.setRecordLength(conf, recordLength)
    conf.set(FileInputFormat.INPUT_DIR, path)
    val inputFormat = if (recordFormat.startsWith("F")) new FixedLengthInputFormat else new VariableLengthInputFormat

    val ct = System.currentTimeMillis
    val jobContext = new JobContextImpl(conf, new JobID((ct / 1000).toString, (ct % 1000).toInt))
    inputFormat.getSplits(jobContext).map(inputSplit =>
      new CobolReaderFactory(InputPart.fromFileSplit(inputSplit.asInstanceOf[FileSplit]), conf, copyBook,bookName,
        split, font, copyBookFormat, binaryFormat, recordFormat, numberType, isTree, nullValue, emptyValue))
  }
}