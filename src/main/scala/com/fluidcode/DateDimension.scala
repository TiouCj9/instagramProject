package com.fluidcode

import com.fluidcode.DateDimensionUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDate


object DateDimension {
  def createDateDimension(spark: SparkSession, startDate: LocalDate, endDate: LocalDate): DataFrame = {
    import spark.implicits._

    val listDays = generateDates(startDate, endDate)

    val dateDimensionList = listDays.map(date =>
      DateDim(
        date.toString,
        getDescription(date),
        date.getDayOfMonth,
        date.getDayOfWeek.toString,
        date.getMonth.toString,
        date.getYear,
        getQuarter(date),
        isWeekend(date),
        isHoliday(date)
      ))
    dateDimensionList.toDF()
  }
}