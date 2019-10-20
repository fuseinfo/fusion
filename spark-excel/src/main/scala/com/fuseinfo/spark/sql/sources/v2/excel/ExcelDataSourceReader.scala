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

import java.text.SimpleDateFormat
import java.util

import com.fuseinfo.spark.utils.{InputPart, SerializableConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, JobID, RecordReader, TaskAttemptContext}
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.{Cell, DateUtil}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._

class ExcelDataSourceReader(dso: DataSourceOptions) extends DataSourceReader {
  private val spark = SparkSession.builder.getOrCreate
  private val hadoopConf = spark.sparkContext.hadoopConfiguration
  private val conf = new SerializableConfiguration(hadoopConf)
  private val path = dso.get("path").orElse("")
  private val sheetName = dso.get("sheet").orElse("0")
  private val dateFormat = dso.get("dateformat").orElse("yyyy-MM-dd")
  private val timestampFormat = {
    val format = dso.get("timestampformat")
    if (format.isPresent) Some(format.get) else None
  }
  private val emptyValue = dso.get("emptyvalue").orElse(null)
  private val inferSchema = dso.getBoolean("inferschema", false)
  private val header = dso.getBoolean("header", false)
  private val skipLinesRaw = dso.getInt("skiplines", 0)
  private val skipLines = if (header) skipLinesRaw + 1 else skipLinesRaw
  conf.set(FileInputFormat.INPUT_DIR, path)

  private val splits = {
    val inputFormat = new FileInputFormat(){
      override def isSplitable(context: JobContext, filename: Path): Boolean = false
      override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Nothing, Nothing] =
        null
    }
    val ct = System.currentTimeMillis
    val jobContext = new JobContextImpl(conf, new JobID((ct / 1000).toString, (ct % 1000).toInt))
    inputFormat.getSplits(jobContext)}

  private val schema = {
    val fieldsOpt = dso.get("fields")
    if (fieldsOpt.isPresent && !inferSchema) {
      StructType(fieldsOpt.get.split("\\,").map(fieldName => StructField(fieldName, StringType, true)))
    } else {
      val splitPath = splits(0).asInstanceOf[FileSplit].getPath
      val splitUri = splitPath.toUri
      val sdf = new SimpleDateFormat(dateFormat)
      val tdf = timestampFormat.map(new SimpleDateFormat(_))
      val fs = FileSystem.get(splitUri, conf)
      val is = fs.open(splitPath)
      val workbook = if (splitPath.getName.endsWith(".xls")) new HSSFWorkbook(is) else new XSSFWorkbook(is)
      val sheet = workbook.getSheet(sheetName) match {
        case null => workbook.getSheetAt(sheetName.toInt)
        case result => result
      }
      val iterator = sheet.iterator()
      0 until skipLinesRaw foreach (_ => iterator.next())

      val (fieldNames, row) = if (fieldsOpt.isPresent) {
        (fieldsOpt.get.split("\\,"), iterator.next())
      } else if (header) {
        val headerRow = iterator.next()
        val size = headerRow.getLastCellNum
        ((for (i <- 0 until size) yield {
          val cell = headerRow.getCell(i)
          if (cell != null && cell.getCellType == Cell.CELL_TYPE_STRING) cell.getStringCellValue else "c" + i
        }).toArray, iterator.next())
      } else {
        val nextRow = iterator.next()
        val size = nextRow.getLastCellNum
        ((0 until size).map("c" + _).toArray, nextRow)
      }

      if (inferSchema) {
        StructType(fieldNames.indices.map{i =>
          val cell = row.getCell(i)
          val dataType = if (cell != null) {
            val cellType = cell.getCellType match {
              case Cell.CELL_TYPE_FORMULA => cell.getCachedFormulaResultType
              case other => other
            }
            cellType match {
              case Cell.CELL_TYPE_BOOLEAN => BooleanType
              case Cell.CELL_TYPE_NUMERIC => if (DateUtil.isCellDateFormatted(cell)) DateType else DoubleType
              case _ => StringType
            }
          } else StringType
          StructField(fieldNames(i), dataType, true)
        })
      } else {
        StructType(fieldNames.map(fieldName => StructField(fieldName, StringType, true)))
      }
    }
  }

  override def readSchema(): StructType = schema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    splits.map(split => new ExcelDataReaderFactory(conf, InputPart.fromFileSplit(split.asInstanceOf[FileSplit]),
      schema, sheetName, skipLines, dateFormat, timestampFormat, emptyValue))
  }
}

class DefaultSource extends DataSourceV2 with ReadSupport {
  def createReader(dso: DataSourceOptions): DataSourceReader = {
    new ExcelDataSourceReader(dso)
  }
}