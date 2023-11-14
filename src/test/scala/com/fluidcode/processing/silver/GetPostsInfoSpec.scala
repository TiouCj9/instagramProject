package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.fluidcode.models.silver.PostsData
import com.fluidcode.processing.bronze.BronzeLayer
import com.fluidcode.processing.silver.GetPostsInfoAndCreateTable._


class GetPostsInfoSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("getPostsInfo should select post info data from Bronze Layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession) //create new bronze table to resolve error Path does not exist:

      import sparkSession.implicits._

      val path = "c:/tmp/StreamSource/silver"
      val bronzeLayer = new BronzeLayer(conf, sparkSession, path)
      bronzeLayer.createBronzeTable()
      Thread.sleep(5000)

      val bronzeData = spark.read.format("delta")
        .load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")

      val result = getPostsInfo(bronzeData)
      val expectedResult = Seq(
        PostsData(comments_disabled = true, 100, "caption_text1", 50, null, "image_id1", is_video = false, null,
          "138289436", "shortcode1", "tag1", 1623779104, "benz")
      ).toDF()
      assert(result.except(expectedResult).isEmpty)
    }
  }
}
