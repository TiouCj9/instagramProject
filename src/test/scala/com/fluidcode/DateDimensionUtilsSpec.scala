package com.fluidcode


import com.fluidcode.DateDimensionUtils._

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
  Given("LocalDate")
  val dateFormat = "MM/dd/yyyy"
  val dtf = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
  val dateString = "06/04/2031"

  "getDescription" should "contain the same result as expectedResult" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("getDescription Is invoked")
    val description = getDescription(test)
    Then("getQuarter should contain the same element as raw data")
    val expectedResult = ("1"+"3"+"M"+"A"+"R"+"C"+"H"+"2"+"0"+"2"+"2")
    expectedResult should contain theSameElementsAs(description)
  }

  "getQuarter" should "Be a Georgian Calendar with holidays" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("getQuarter Is invoked")
    val quarter = getQuarter(test)
    Then("getQuarter should contain the same element as raw data")
    val expectedResult = ("Q" + "1")
    expectedResult should contain theSameElementsAs(quarter)
  }

  "defWeekend" should "Define if a day is a weekend or not" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("defWeekend Is invoked")
    val isWeek = defWeekend(test)
    Then("defWeekend should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(isWeek).toDF.collect())
  }
  
  "isLeapYear" should "Define if a day is a leap year or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isLeapYear Is invoked")
    val isLeap = isLeapYear(test)
    Then("isLeapYear should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(isLeap).toDF.collect())

  }

  "IsFirstDayOfYearAndIsIndependencyDay" should "Define if it is the first day in a year and indepndcy day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isFirstDayOfYearAndIsIndependencyDay Is invoked")
    val firstDayOfYearAndIsIndependencyDay = isFirstDayOfYearAndIsIndependencyDay(test)
    Then("isFirstDayOfYearAndIsIndependencyDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(firstDayOfYearAndIsIndependencyDay).toDF.collect())
  }

  "isLabourDay" should "Define if a day is labour day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isLabourDay Is invoked")
    val issLabourDay = isLabourDay(test)
    Then("issLabourDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issLabourDay).toDF.collect())
  }

  "isVictoryDay" should "Define if a day is vicotry day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isVictoryDay Is invoked")
    val issVictoryDay = isVictoryDay(test)
    Then("isVictoryDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issVictoryDay).toDF.collect())
  }

  "isBastilleDay" should "Define if a day is bastille day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isBastilleDay Is invoked")
    val issBastilleDay = isBastilleDay(test)
    Then("issBastilleDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issBastilleDay).toDF.collect())
  }

  "isAssumptionofMary" should "Define if a day is assumption of mary day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isAssumptionofMary Is invoked")
    val issAssumptionofMary = isAssumptionofMary(test)
    Then("issAssumptionofMary should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issAssumptionofMary).toDF.collect())
  }

  "isAllSaintsDay" should "Define if a day is allSaint day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isAllSaintsDay Is invoked")
    val issAllSaintsDay = isAllSaintsDay(test)
    Then("issAllSaintsDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issAllSaintsDay).toDF.collect())
  }

  "isArmisticeDay" should "Define if a day is Armistice Day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isArmisticeDay Is invoked")
    val issArmisticeDay = isArmisticeDay(test)
    Then("issLabourDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issArmisticeDay).toDF.collect())
  }

  "isChristmasDay" should "Define if a day is a christmas day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isChristmasDay Is invoked")
    val issChristmasDay = isChristmasDay(test)
    Then("issChristmasDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issChristmasDay).toDF.collect())
  }

  "isEasterDay" should "Define if a day is an easter sunday or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isEasterDay Is invoked")
    val issEasterDay = isEasterDay(test)
    Then("issEasterDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issEasterDay).toDF.collect())
  }

  "isEasterMonday" should "Define if a day is an easter sunday or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isEasterMonday Is invoked")
    val issEasterMonday = isEasterMonday(test)
    Then("issEasterMonday should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issEasterMonday).toDF.collect())
  }

  "isAscensionDay" should "Define if a day is an ascension day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isAscensionDay Is invoked")
    val issAscensionDay = isAscensionDay(test)
    Then("issAscensionDay should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issAscensionDay).toDF.collect())
  }

  "isWhitMonday" should "Define if a day is an Whitmonday day or no" in {

    Given("LocalDate")
    val test = java.time.LocalDate.parse(dateString, dtf)
    When("isWhitMonday Is invoked")
    val issWhitMonday = isWhitMonday(test)
    Then("issWhitMonday should contain the same element as test")
    val expectedResult = true
    Seq(expectedResult).toDF.collect should contain theSameElementsAs(Seq(issWhitMonday).toDF.collect())
  }
}