package com.fluidcode.processing.bronze

import com.fluidcode.configuration.Configuration
import com.fluidcode.models.bronze._
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

      import sparkSession.implicits._

      val path = "testSample"

      val sampleDf = Seq(
        Data(Array(GraphImagesElements("Graph1", CommentsData(Array(DataElements(1623779105, "1382894360", OwnerData("138289436000", "https://profile_pic_url1", "mehrez"), "comment_text1"))),
          comments_disabled = true, DimensionsData(1080, 1920), "https://instagram.url1", Likes(100), Captions(Array(EdgesElements(NodeData("caption_text1")))),
          Comments(50), null, "image_id1", is_video = false, null, "s564gsd", Owner("138289436"), "shortcode1", Array("tag1"),
          1623779104, Array(ThumbnailElements(150, 150, "https://instagramthumbnail_src1")), "https://thumbnail_src1", Array("url1", "url2"),
          "benz")),
          ProfileInfo(1623779107, Info("biography1", 1000, 500, "full_name1", "138289cc", is_business_account = true, is_joined_recently = false, is_private = false, 100,
          "https://profile_pic_url1"), "benz"))
      ).toDF()

      sampleDf.write.mode("overwrite").json(path)

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
