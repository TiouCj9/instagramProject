package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import com.fluidcode.processing.bronze.BronzeLayer
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
class BronzeLayerSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("createBronzeTable should create  BronzeTable from Ingestion Layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession)
      
      val path = "phil.coutinho-1-test.json"
      val bronzeLayer = new BronzeLayer(conf, sparkSession, path)
      bronzeLayer.createBronzeTable()
      Thread.sleep(5000)

      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
      val expectedResult = spark.read.option("multiLine", true).json(path)
      assert(result.except(expectedResult).isEmpty)
    }
  }
}
