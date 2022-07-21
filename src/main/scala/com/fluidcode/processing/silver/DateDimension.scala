package com.fluidcode.processing.silver

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import DateDimensionUtils._
import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

object DateDimension {

  def CreateDateDimensionTable(spark: SparkSession, conf: Configuration, dateDimension : DataFrame): Unit = {
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val startDate = LocalDate.parse("2022-01-01", formatter)
    val endDate = LocalDate.parse("2050-01-01", formatter)

    createDateDimension(spark: SparkSession, startDate: LocalDate, endDate: LocalDate)
      .write
      .format("delta")
      .save(s"${conf.rootPath}/${conf.database}/${conf.dateDimensionTable}")
  }

  }

