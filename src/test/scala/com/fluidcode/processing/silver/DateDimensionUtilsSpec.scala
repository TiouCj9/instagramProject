package com.fluidcode.processing.silver

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.fluidcode.processing.silver.DateDimensionUtils._
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateDimensionUtilsSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("projectInstagram")
    .getOrCreate()
  import spark.implicits._

  "getDescription" should "Return date description" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-01-01", formatter)
    When("getDescription Is invoked")
    val result = Seq(getDescription(date)).toDF()
    Then("expectedResult should contain the same element as result")
    val expectedResult = Seq(("SATURDAY, JANUARY 1, 2022")).toDF()
    expectedResult.collect() should contain theSameElementsAs(result.collect())
  }

  "getQuarter" should "Return in which quarter of the year" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-01-01", formatter)
    When("getQuarter Is invoked")
    val result = Seq(getQuarter(date)).toDF()
    Then("expectedResult should contain the same element as result")
    val expectedResult = Seq(("Q1")).toDF()
    expectedResult.collect should contain theSameElementsAs (result.collect())
  }

  "isWeekend" should "Return if a date is a weekend or not" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-01-01", formatter)
    When("isWeekend Is invoked")
    val result = Seq(isWeekend(date)).toDF()
    Then("expectedResult should contain the same element as result")
    val expectedResult = true
    Seq(expectedResult).toDF().collect should contain theSameElementsAs (result.collect())
  }

  "isEasterMonday" should "Return if a date is an easter monday day or not" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-04-18", formatter)
    When("isEasterMonday Is invoked")
    val result = Seq(isEasterMonday(date))
    Then("expectedResult should contain the same element as result")
    val expectedResult = Seq(LocalDate.parse("2022-04-18", formatter))
    expectedResult should contain theSameElementsAs result
  }

  "matchingEasterMonday" should "Return full date with description and holidays" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-04-18", formatter)
    When("matchingEasterMonday Is invoked")
    val result = Seq(matchingEasterMonday(date)).toDF()
    Then("expectedResult should contain the same element as result")
    val expectedResult = Seq(("18-4")).toDF()
    expectedResult.collect should contain theSameElementsAs (result.collect())
  }

  "isAscensionDay" should "Return if a date is an ascension day or not" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-05-26", formatter)
    When("isAscensionDay Is invoked")
    val result = Seq(isAscensionDay(date)).toDF()
    Then("expectedResult should contain the same element as result")
    val expectedResult = Seq(("26-5")).toDF()
    expectedResult.collect should contain theSameElementsAs (result.collect())
  }

  "isWhitMonday" should "Return if a date is a whit monday day or not" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-06-06", formatter)
    When("isWhitMonday Is invoked")
    val result = Seq(isWhitMonday(date)).toDF()
    Then("expectedResult should contain the same element as result")
    val expectedResult = Seq(("6-6")).toDF()
    expectedResult.collect should contain theSameElementsAs result.collect()
  }

  "fixDateFormat" should "Fix Dates Format by adding 0 to each month and day" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    When("fixDateFormat Is invoked")
    val result = Seq(fixDateFormat(month = 1, day = 1, year = 2022))
    Then("expectedResult should contain the same element as result")
    val expectedResult = LocalDate.parse("2022-01-01", formatter)
    Seq(expectedResult) should contain theSameElementsAs result
  }

  "isHoliday" should "Define if a day is a holiday or not" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val date = LocalDate.parse("2022-06-06", formatter)
    When("isHoliday Is invoked")
    val result = Seq(isHoliday(date)).toDF()
    Then("expectedResult should contain the same element as result")
    val expectedResult = true
    Seq(expectedResult).toDF().collect() should contain theSameElementsAs result.collect()

  }

  "createDateDimension" should "Return full date with description and holidays" in {
    Given("LocalDate")
    val dateFormat = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val startDate = LocalDate.parse("2022-04-17", formatter)
    val endDate = LocalDate.parse("2022-04-19", formatter)
    When("showDate Is invoked")
    val result = createDateDimension(spark, startDate,endDate)
    Then("showDate should contain the same element as raw data")
    //Seq(expectedResult).toDF().collect should contain theSameElementsAs (result.collect())
    result.show()

  }

}
