package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession

case class PostsInfoData(comments_disabled: Boolean, edge_media_preview_like_count: Long, edge_media_to_caption_edges_node_text: String,
                         edge_media_to_comment_count: Long, gating_info: String, id: String, is_video: Boolean, location: String,
                         owner_id: String, shortcode: String, tags: String, taken_at_timestamp: Long, username: String)

class GetPostsInfoSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("getPostsInfo should create  BronzeTable from Ingestion Layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession)
      import sparkSession.implicits._

      val silverInput = spark.read.option("multiLine", value = true).json("sample.json")
      val createPostInfoTable = new GetPostsInfo(silverInput, conf)
      createPostInfoTable.createPostsInfoTable()
      Thread.sleep(5000)

      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.postInfoTable}")
      val expectedResult = Seq(
        PostsInfoData(comments_disabled = true, 100, "caption_text1", 50, null, "image_id1", is_video = false, null, "138289436", "shortcode1", "tag1", 1623779104, "benz")
      ).toDF()
      assert(result.except(expectedResult).isEmpty)
    }
  }
}
