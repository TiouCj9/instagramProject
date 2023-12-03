package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.SparkSession
import com.fluidcode.processing.silver.CreateDateDimensionsTable.createDateDimensionsTable

object Silver {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder()
      .appName("instagram_pipeline")
      .getOrCreate()

    val conf = Configuration(args(0))
    val startDate = args(1)
    val periodInDays = args(2).toInt

    createDateDimensionsTable(startDate, periodInDays, conf, spark)
  }
}