package com.fluidcode.processing.silver

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.fluidcode.processing.silver.DateDimension._
import com.fluidcode.models._


class DateDimensionSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  override def afterEach(): Unit = {
    super.afterEach()
    spark.catalog
      .listDatabases()
      .filter(_.name != "default")
      .collect()
      .map(db => spark.sql(s"drop database if exists ${db.name} cascade"))
  }

  test("CreateDateDimensionTable should create dateDimension Table" ) {
    withTempDir { dir =>
      val sparkSession = spark
      import sparkSession.implicits._
      val conf = Configuration(dir.toString)
      val dateFormat = "yyyy-MM-dd"
      val formatter = DateTimeFormatter.ofPattern(dateFormat)
      val startDate = LocalDate.parse("2022-04-17", formatter)
      val endDate = LocalDate.parse("2022-04-19", formatter)

      val dateDimension = Seq((startDate,endDate)).toDF()
      val result = Seq(CreateDateDimensionTable(sparkSession, conf, dateDimension:DataFrame))
      Thread.sleep(5000)

      val expectedResult = Seq(
        DateDim(date = "2022-04-17",description = "SUNDAY, APRIL 17, 2022",dayOfMonth = 17, dayOfWeek = "SUNDAY",
        month = "APRIL", year = 2022, quarter = "Q2", isWeekend = true, isHoliday = true),
        DateDim(
          date = "2022-04-18",description = "MONDAY, APRIL 18, 2022",dayOfMonth = 18, dayOfWeek = "MONDAY",
          month = "APRIL", year = 2022, quarter = "Q2", isWeekend = false, isHoliday = false),
        DateDim(
          date = "2022-04-19",description = "TUESDAY, APRIL 19, 2022",dayOfMonth = 19, dayOfWeek = "TUESDAY",
          month = "APRIL", year = 2022, quarter = "Q2", isWeekend = false, isHoliday = false)
      ).toDF()

      expectedResult.collect() contains result
    }
  }
}