package com.fuseinfo.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object excel {

  implicit class ExcelDataFrameReader(reader: DataFrameReader) {
    def cobol: String => DataFrame = reader.format("com.fuseinfo.spark.sql.sources.v2.excel").load
  }
}
