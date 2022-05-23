package com.fluidcode


import com.fluidcode.DateDimension._
import com.fluidcode.Models._
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateDimensionSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  "DateDimenesion" should "Be a Georgian Calendar with holidays" in {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("flattenDataFrame_Test")
      .getOrCreate()
    import spark.implicits._


    Given("the raw data")
    //val calendar =
    When("DateDimenesionSpec Is invoked")
    //val dateDimenesion = generateDates(calendar)
    Then("DateDimenesion should contain the same element as raw data")
    //val expectedResult = Seq().toDF()

  }
}