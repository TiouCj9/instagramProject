package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import com.fluidcode.processing.bronze.BronzeLayer
import com.fluidcode.processing.silver.GetPostsInfoAndCreateTable._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession

class GetPostsInfoAndCreateTableSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("getPostsInfo should create post info table from Bronze Layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession)

      val path = "c:/tmp/StreamSource/silver"
      val bronzeLayer = new BronzeLayer(conf, sparkSession, path)
      bronzeLayer.createBronzeTable()

      val bronzeData = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
      val postsInfo = getPostsInfo(bronzeData)
      createPostsInfoTable(postsInfo, conf)
      Thread.sleep(5000)

      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.postInfoTable}")
      val expectedResult = spark.read.option("multiLine", true).json("postsInfo.json")
      assert(result.except(expectedResult).isEmpty)
    }
  }
}
