package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.fluidcode.processing.bronze.BronzeLayer._
class BronzeLayerSpec extends QueryTest
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

  test("createBronzeTable should create  BronzeTable from Ingestion Layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession)
      import sparkSession.implicits._
      val path = "phil.coutinho-1-test.json"
      createBronzeTable(conf, sparkSession,path)
      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
      val expectedResult = spark.read.option("multiLine", true).json(path)
      assert(result.except(expectedResult).isEmpty)
    }
  }
}
