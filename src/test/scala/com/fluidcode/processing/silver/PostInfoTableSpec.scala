package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import com.fluidcode.processing.bronze.BronzeLayer
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.fluidcode.processing.silver.PostInfoTableUtils.getPostInfoData
import org.apache.spark.sql.functions.col

class PostInfoTableSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("createPostInfoTable should create comments table from Bronze layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      import sparkSession.implicits._
      val conf = Configuration(dir.toString)
      conf.init(spark)   // creation des tables

      val path = "phil.coutinho-1-test.json"
      val bronzeLayer = new BronzeLayer(conf, sparkSession, path)
      bronzeLayer.createBronzeTable()
      Thread.sleep(5000)
      val createPostInfoTable = new PostInfoTable(sparkSession, conf)
      createPostInfoTable.createPostInfoTable().processAllAvailable()
      Thread.sleep(5000)

      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.postInfoTable}")
      val rawData = spark.read
        .option("multiLine", true)
        .json(path)

      val expectedResult = getPostInfoData(rawData)
        .select(
          col("comments_disabled"),
          col("dimensions_height"),
          col("dimensions_width"),
          col("display_url"),
          col("edge_media_preview_like_count"),
          col("text"),
          col("edge_media_to_comment_count"),
          col("gating_info"),
          col("id"),
          col("is_video"),
          col("location"),
          col("media_preview"),
          col("owner_id"),
          col("shortcode"),
          col("tags"),
          col("taken_at_timestamp"),
          col("thumbnail_resources_config_height"),
          col("thumbnail_resources_config_width"),
          col("thumbnail_resources_config_src"),
          col("urls"),
          col("username")
        )
      assert(result.except(expectedResult).isEmpty)
    }
  }
}