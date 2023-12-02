package com.fluidcode.processing.silver

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import com.fluidcode.processing.silver.CreateDateDimensionsTable._
import com.fluidcode.models.silver.SilverDateDimensions
import com.fluidcode.configuration.Configuration

class CreateDateDimensionsTableSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession {

  test("calculateEasterDate should return easter day date for any given year"){
    val SparkSession = spark
    import SparkSession.implicits._

    val years= Seq(2023, 2024)
    val result = years.map(year => (year, calculateEasterDate(year))).toDF("Year", "EasterDate")

    val expectedResult = Seq(
      ("2023", "2023-04-09"),
      ("2024", "2024-03-31")
    ).toDF("Year","EasterDate")

    assert(result.except(expectedResult).isEmpty)
  }

  test("getDateDimensions should generate date dimensions for a specific period"){
    val SparkSession = spark
    import SparkSession.implicits._

    val startDate = "2024-03-30"
    val days = 3

    val result = getDateDimensions(startDate, days, spark)

    val expectedResult = Seq(
      SilverDateDimensions("2024-03-30", "Saturday, 30 March 2024", 30, "Saturday", "March", 2024, 1, isWeekend = true, isHoliday = false),
      SilverDateDimensions("2024-03-31", "Sunday, 31 March 2024", 31, "Sunday", "March", 2024, 1, isWeekend = true, isHoliday = true),
      SilverDateDimensions("2024-04-01", "Monday, 01 April 2024", 1, "Monday", "April", 2024, 2, isWeekend = false, isHoliday = false)
    ).toDF()

    assert(result.except(expectedResult).isEmpty)
  }

  test("createDateDimensionsTab should create date dimensions table for a specific period, starting from a well-defined start date" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession)

      import sparkSession.implicits._

      val startDate = "2024-03-30"
      val days = 3

      createDateDimensionsTable(startDate, days, conf, spark)
      Thread.sleep(5000)

      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.dateDimensionsTable}")

      val expectedResult = Seq(
        SilverDateDimensions("2024-03-30", "Saturday, 30 March 2024", 30, "Saturday", "March", 2024, 1, isWeekend = true, isHoliday = false),
        SilverDateDimensions("2024-03-31", "Sunday, 31 March 2024", 31, "Sunday", "March", 2024, 1, isWeekend = true, isHoliday = true),
        SilverDateDimensions("2024-04-01", "Monday, 01 April 2024", 1, "Monday", "April", 2024, 2, isWeekend = false, isHoliday = false)
      ).toDF()

      assert(result.except(expectedResult).isEmpty)
    }
  }
}