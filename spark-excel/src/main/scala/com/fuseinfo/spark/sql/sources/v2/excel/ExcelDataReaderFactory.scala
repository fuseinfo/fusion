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

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.fuseinfo.spark.utils.FilePart
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.{Cell, DateUtil}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._

import scala.util.Try

class ExcelDataReaderFactory(conf:Configuration, filePart:FilePart, schema:StructType, sheetName:String, skipLines:Int,
                             dateFormat:String, timestampFormat:Option[String], emptyValue:String)
  extends DataReaderFactory[Row] {

  override def createDataReader(): DataReader[Row] =
    new ExcelDataReader(conf, filePart, schema, sheetName, skipLines, dateFormat, timestampFormat, emptyValue)
}

class ExcelDataReader(val conf:Configuration, filePart:FilePart, val schema:StructType, sheetName:String, skipLines:Int,
                      dateFormat:String, timestampFormat:Option[String], emptyValue:String) extends DataReader[Row] {
  private val uri = filePart.file
  private val sdf = new SimpleDateFormat(dateFormat)
  private val tdf = timestampFormat.map(new SimpleDateFormat(_))
  private val fs = FileSystem.get(uri, conf)
  private val is = fs.open(new Path(uri))
  private val workbook = if (uri.toString.endsWith(".xls")) new HSSFWorkbook(is) else new XSSFWorkbook(is)
  private val sheet = workbook.getSheet(sheetName) match {
    case null => workbook.getSheetAt(sheetName.toInt)
    case result => result
  }
  private val rowIterator = sheet.iterator()
  0 until skipLines foreach(_ => rowIterator.next())
  private val fields = schema.fields

  override def next(): Boolean = rowIterator.hasNext

  override def get(): Row = {
    val row = rowIterator.next()
    Row(fields.indices.map{i =>
      val cell = row.getCell(i)
      if (cell == null) null else {
        val cellType = cell.getCellType match {
          case Cell.CELL_TYPE_FORMULA => cell.getCachedFormulaResultType
          case other => other
        }
        Try{(cellType, fields(i).dataType) match {
          case (Cell.CELL_TYPE_STRING, BooleanType) => cell.getStringCellValue.toBoolean
          case (Cell.CELL_TYPE_STRING, DateType) => new java.sql.Date(sdf.parse(cell.getStringCellValue).getTime)
          case (Cell.CELL_TYPE_STRING, DoubleType) => cell.getStringCellValue.toDouble
          case (Cell.CELL_TYPE_STRING, FloatType) => cell.getStringCellValue.toFloat
          case (Cell.CELL_TYPE_STRING, IntegerType) => cell.getStringCellValue.toInt
          case (Cell.CELL_TYPE_STRING, LongType) => cell.getStringCellValue.toLong
          case (Cell.CELL_TYPE_STRING, ShortType) => cell.getStringCellValue.toShort
          case (Cell.CELL_TYPE_STRING, StringType) => Try(cell.getStringCellValue).getOrElse(emptyValue)
          case (Cell.CELL_TYPE_STRING, TimestampType) =>
            val cellValue = cell.getStringCellValue
            tdf match {
              case Some(df) => new Timestamp(df.parse(cell.getStringCellValue).getTime)
              case None => Timestamp.valueOf(cellValue)
            }
          case (Cell.CELL_TYPE_STRING, dec: DecimalType) => new java.math.BigDecimal(cell.getStringCellValue.replaceAll(",", ""))
          case (Cell.CELL_TYPE_NUMERIC, DateType) => new java.sql.Date(DateUtil.getJavaDate(cell.getNumericCellValue).getTime)
          case (Cell.CELL_TYPE_NUMERIC, DoubleType) => cell.getNumericCellValue
          case (Cell.CELL_TYPE_NUMERIC, FloatType) => cell.getNumericCellValue.toFloat
          case (Cell.CELL_TYPE_NUMERIC, IntegerType) => cell.getNumericCellValue.toInt
          case (Cell.CELL_TYPE_NUMERIC, LongType) => cell.getNumericCellValue.toLong
          case (Cell.CELL_TYPE_NUMERIC, ShortType) => cell.getNumericCellValue.toShort
          case (Cell.CELL_TYPE_NUMERIC, StringType) => Try(cell.getNumericCellValue.toString).getOrElse(emptyValue)
          case (Cell.CELL_TYPE_NUMERIC, TimestampType) => new Timestamp(DateUtil.getJavaDate(cell.getNumericCellValue).getTime)
          case (Cell.CELL_TYPE_NUMERIC, dec: DecimalType) => new java.math.BigDecimal(cell.getNumericCellValue)
          case (Cell.CELL_TYPE_BOOLEAN, BooleanType) => cell.getBooleanCellValue
          case (Cell.CELL_TYPE_BOOLEAN, StringType) => Try(cell.getBooleanCellValue.toString).getOrElse(emptyValue)
          case (_, StringType) => emptyValue
          case (_, _) => null
        }
        }.getOrElse(null)
      }
    }:_*)
  }

  override def close(): Unit = {
    workbook.close()
    is.close()
  }
}