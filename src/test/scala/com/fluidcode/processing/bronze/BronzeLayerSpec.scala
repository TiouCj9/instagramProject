package com.fluidcode.processing.bronze

import com.fluidcode.configuration.Configuration
import com.fluidcode.models.bronze.Data
import org.apache.spark.sql.{Encoders, QueryTest}
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession

class BronzeLayerSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("createBronzeTable should create BronzeTable from Ingestion Layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession)

      val path = "c:/tmp/StreamSource/bronze"

      val bronzeLayer = new BronzeLayer(conf, sparkSession, path)
      bronzeLayer.createBronzeTable()
      Thread.sleep(5000)

      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")

      val bronzeSchema = Encoders.product[Data].schema
      val expectedResult = spark.read.schema(bronzeSchema).json(path)
      assert(result.except(expectedResult).isEmpty)
    }
  }
}
