package com.fluidcode.processing.silver

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.fluidcode.models.DateDim
import com.fluidcode.processing.silver.DateDimensionUtils._
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{QueryTest, SparkSession}

class DateDimensionUtilsSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession {

  test("getDescription should Return date description") {
    withTempDir { dir =>
      val sparkSession = spark
      import sparkSession.implicits._
      val dateFormat = "yyyy-MM-dd"
      val formatter = DateTimeFormatter.ofPattern(dateFormat)
      val date = LocalDate.parse("2022-01-01", formatter)
      val result = getDescription(date)
      val expectedResult = ("SATURDAY, JANUARY 1, 2022")

      assert (expectedResult == result)
    }
  }

  test("getQuarter Return in which quarter of the year") {

    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-01-01", formatter)
    val result = getQuarter(date)
    val expectedResult = ("Q1")
    assert (expectedResult == result)
  }
  test("isWeekend Return if a date is a weekend or not") {

    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-01-01", formatter)
    val result = isWeekend(date).toString
    val expectedResult = "true"
    assert (expectedResult == result)
  }
  test("isEasterMonday Return if a date is an easter monday day or not") {
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-04-18", formatter)
    val result = isEasterMonday(date).toString
    val expectedResult = LocalDate.parse("2022-04-18", formatter).toString
    assert(expectedResult == result)
  }

  test("matchingEasterMonday Return full date with description and holidays") {
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-04-18", formatter)
    val result = matchingEasterMonday(date)
    val expectedResult = ("18-4")
    assert (expectedResult == result)
  }

  test("isAscensionDay Return if a date is an ascension day or not") {

    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-05-26", formatter)
    val result = isAscensionDay(date)
    val expectedResult = ("26-5")
    assert (expectedResult == result)
  }

  test("isWhitMonday Return if a date is a whit monday day or not") {
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-06-06", formatter)
    val result = isWhitMonday(date)
    val expectedResult = ("6-6")
    assert (expectedResult == result)
  }

  test("fixDateFormat Fix Dates Format by adding 0 to each month and day") {
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val result = fixDateFormat(month = 1, day = 1, year = 2022)
    val expectedResult = LocalDate.parse("2022-01-01", formatter)
    assert (expectedResult == result)
  }

  test("isHoliday Define if a day is a holiday or not") {
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-06-06", formatter)
    val result = isHoliday(date).toString
    val expectedResult = "true"
    assert (expectedResult == result)
  }

  test("createDateDimension Return full date with description and holidays") {
    val sparkSession = spark
    import sparkSession.implicits._
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val startDate = LocalDate.parse("2022-04-17", formatter)
    val endDate = LocalDate.parse("2022-04-19", formatter)
    val result = createDateDimension(spark, startDate, endDate)
    val expectedResult = Seq(
      DateDim(date = "2022-04-17", description = "SUNDAY, APRIL 17, 2022", dayOfMonth = 17, dayOfWeek = "SUNDAY",
        month = "APRIL", year = 2022, quarter = "Q2", isWeekend = true, isHoliday = true),
      DateDim(
        date = "2022-04-18", description = "MONDAY, APRIL 18, 2022", dayOfMonth = 18, dayOfWeek = "MONDAY",
        month = "APRIL", year = 2022, quarter = "Q2", isWeekend = false, isHoliday = true),
      DateDim(
        date = "2022-04-19", description = "TUESDAY, APRIL 19, 2022", dayOfMonth = 19, dayOfWeek = "TUESDAY",
        month = "APRIL", year = 2022, quarter = "Q2", isWeekend = false, isHoliday = false)
    ).toDF()
    assert(expectedResult.collect() sameElements result.collect())
  }

}
