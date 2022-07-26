package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.fluidcode.processing.bronze.BronzeLayer._
class BronzeLayerSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("createBronzeTable should create  BronzeTable from Ingestion Layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession)
      createBronzeTable(conf, sparkSession)
      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
      val expectedResult = spark.read.option("multiLine", true).json("phil.coutinho-1-test.json")
      assert(result.except(expectedResult).isEmpty)
    }
  }
}
