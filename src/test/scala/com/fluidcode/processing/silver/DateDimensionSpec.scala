package com.fluidcode.processing.silver

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.fluidcode.processing.silver.DateDimension.createDateDimension
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateDimensionSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("projectInstagram")
    .getOrCreate()

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
