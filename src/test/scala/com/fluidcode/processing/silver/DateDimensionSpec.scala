package com.fluidcode.processing.silver

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.fluidcode.processing.silver.DateDimension._
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


}
