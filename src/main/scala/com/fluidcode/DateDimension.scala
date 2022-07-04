package com.fluidcode

import com.fluidcode.DateDimensionUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDate

case class DateDimension(date : String, description : String, dayOfMonth: Int, dayOfWeek: String, month: String,  year: Int, quarter: String, isWeekend: Boolean, isHoliday: Boolean)

object DateDimension {

  def isHoliday(date: LocalDate) : Boolean = {

    (date.getDayOfWeek.toString , date.getDayOfMonth+ "-" + date.getMonthValue.toString) match {
      case (anyday , "1-1") if anyday == date.getDayOfWeek.toString => true
      case (anyday, "5-5") if anyday == date.getDayOfWeek.toString => true
      case (anyday, "8-7") if anyday == date.getDayOfWeek.toString => true
      case (anyday, ascension) if anyday == date.getDayOfWeek.toString && ascension == isAscensionDay(date) => true
      case (anyday, easter) if anyday == date.getDayOfWeek.toString && easter == matchingEasterMonday(date) => true
      case (anyday, whit) if anyday == date.getDayOfWeek.toString && whit == isWhitMonday(date) => true
      case _ => false
    }
  }

  def createDateDimension(spark: SparkSession, startDate: LocalDate, endDate: LocalDate): DataFrame = {
    import spark.implicits._

    val listDays = generateDates(startDate, endDate)

    val dateDimensionList = listDays.map(date =>
      DateDimension(
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