package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.types.StringType


object CreateDateDimensionsTable {

  def calculateEasterDate(year: Int): LocalDate = {
    val a = year % 19
    val b = year / 100
    val c = year % 100
    val d = b / 4
    val e = b % 4
    val f = (b + 8) / 25
    val g = (b - f + 1) / 3
    val h = (19 * a + b - d - g + 15) % 30
    val i = c / 4
    val k = c % 4
    val l = (32 + 2 * e + 2 * i - h - k) % 7
    val m = (a + 11 * h + 22 * l) / 451
    val month = (h + l - 7 * m + 114) / 31
    val day = ((h + l - 7 * m + 114) % 31) + 1

    LocalDate.of(year, month, day)
  }

  def getDateDimensions(startDate: String, periodInDays: Long, spark: SparkSession): DataFrame = {

    import spark.implicits._
    val datesSeq = (0L until periodInDays).map(LocalDate.parse(startDate).plusDays)

    val datesDF = datesSeq.toDF("date")

    val isEasterDayUDF = udf((dateString: String) => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val date = LocalDate.parse(dateString, formatter)
      val year = date.getYear
      val easter = calculateEasterDate(year)
      date.isEqual(easter)
    })

    val isPentecostDayUDF = udf((dateString: String) => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val date = LocalDate.parse(dateString, formatter)
      val year = date.getYear
      val easter = calculateEasterDate(year)
      date.isEqual(easter.plusDays(50))
    })

    val isAscensionDayUDF = udf((dateString: String) => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val date = LocalDate.parse(dateString, formatter)
      val year = date.getYear
      val easter = calculateEasterDate(year)
      date.isEqual(easter.plusDays(40))
    })

    val dateDimensionDF = datesDF
      .withColumn("date",col("date").cast(StringType))
      .withColumn("description", date_format(to_date($"date", "yyyy-MM-dd"), "EEEE, dd MMMM yyyy"))
      .withColumn("dayOfMonth", dayofmonth(col("date")))
      .withColumn("dayOfWeek", date_format(col("date"), "EEEE"))
      .withColumn("month",  date_format(to_date($"date", "yyyy-MM-dd"), "MMMM"))
      .withColumn("year", year(col("date")))
      .withColumn("quarter", quarter(col("date")))
      .withColumn("isWeekend",
        when(col("dayOfWeek")==="Sunday" || col("dayOfWeek")==="Saturday", true)
          .otherwise(false))
      .withColumn("isHoliday",
        when((col("dayOfMonth")=== 1 && (col("month")=== "January" || col("month")=== "May"))
          || col("dayOfMonth")=== 8 && col("month")=== "May"
          || col("dayOfMonth")=== 14 && col("month")=== "July"
          || col("dayOfMonth")=== 15 && col("month")=== "August"
          || col("dayOfMonth")=== 1 && col("month")=== "November"
          || col("dayOfMonth")=== 11 && col("month")=== "November"
          || col("dayOfMonth")=== 25 && col("month")=== "December"
          || isEasterDayUDF(col("date"))
          || isPentecostDayUDF(col("date"))
          || isAscensionDayUDF(col("date")), true)
        .otherwise(false))

    dateDimensionDF
  }

  def createDateDimensionsTable(startDate: String, periodInDays: Int, conf: Configuration, spark: SparkSession): Unit = {

    val dateDimensions = getDateDimensions(startDate, periodInDays, spark)

    dateDimensions.write
      .option("checkpointLocation", s"${conf.checkpointDir}/${conf.dateDimensionsTable}")
      .format("delta")
      .mode("append")
      .save(s"${conf.rootPath}/${conf.database}/${conf.dateDimensionsTable}")
  }
}