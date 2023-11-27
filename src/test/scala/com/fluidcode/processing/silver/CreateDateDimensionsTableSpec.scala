package com.fluidcode.processing.silver

import com.fluidcode.models.bronze._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import com.fluidcode.processing.silver.CreateDateDimensionsTable._
import com.fluidcode.models.silver.SilverDateDimensions
import com.fluidcode.configuration.Configuration
import com.fluidcode.processing.bronze.BronzeLayer


class CreateDateDimensionsTableSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession {

  test("getDateDimensions should select data dimensions from Bronze Layer"){
    val SparkSession = spark
    import SparkSession.implicits._

    spark.conf.set(
      "spark.sql.optimizer.excludedRules",
      "org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin"
    )

    val sampleDf = Seq(
      Data(
        Array(
          GraphImagesElements(
            "Graph1", CommentsData(
              Array(DataElements(1623779105, "1382894360", OwnerData("138289436000", "https://profile_pic_url1", "mehrez"), "comment_text1"))
            ),
            comments_disabled = true, DimensionsData(1080, 1920), "https://instagram.url1", Likes(100), Captions(
              Array(EdgesElements(
                NodeData("caption_text1")
              )
              )
            ),
            Comments(50), null, "image_id1", is_video = false, null, "s564gsd", Owner("138289436"), "shortcode1", Array("tag1"),
            1623779104, Array(ThumbnailElements(150, 150, "https://instagramthumbnail_src1")), "https://thumbnail_src1", Array("url1", "url2"),
            "benz"
          )
        ),
        ProfileInfo(
          1623779107, Info(
            "biography1", 1000, 500, "full_name1", "138289cc", is_business_account = true, is_joined_recently = false, is_private = false, 100,
            "https://profile_pic_url1"
          ), "benz")
      )
    ).toDF()

    val result = getDateDimensions(sampleDf)

    val expectedResult = Seq(SilverDateDimensions(1623779105, 1623779104, 1623779107)).toDF()

    assert(result.except(expectedResult).isEmpty)
  }

  test("createDateDimensionsTable should create data dimensions table from Bronze Layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      val conf = Configuration(dir.toString)
      conf.init(sparkSession)

      import sparkSession.implicits._

      val path = "testSample"

      val sampleDf = Seq(
        Data(Array(GraphImagesElements(
          "Graph1", CommentsData(Array(DataElements(
            1623779105, "1382894360", OwnerData("138289436000", "https://profile_pic_url1", "mehrez"), "comment_text1")
          )),
          comments_disabled = true, DimensionsData(1080, 1920), "https://instagram.url1", Likes(100),
          Captions(Array(EdgesElements(NodeData("caption_text1")))),
          Comments(50), null, "image_id1", is_video = false, null, "s564gsd", Owner("138289436"), "shortcode1", Array("tag1"),
          1623779104, Array(ThumbnailElements(150, 150, "https://instagramthumbnail_src1")), "https://thumbnail_src1",
          Array("url1", "url2"), "benz")),
          ProfileInfo(1623779107, Info("biography1", 1000, 500, "full_name1", "138289cc", is_business_account = true, is_joined_recently = false, is_private = false, 100,
            "https://profile_pic_url1"), "benz"))
      ).toDF()

      sampleDf.write.mode("overwrite").json(path)

      val bronzeLayer = new BronzeLayer(conf, sparkSession, path)
      bronzeLayer.createBronzeTable()

      createDateDimensionsTable(conf, spark)
      Thread.sleep(5000)

      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.dateDimensionsTable}")
      val expectedResult = Seq(SilverDateDimensions(1623779105, 1623779104, 1623779107)).toDF()
      assert(result.except(expectedResult).isEmpty)
    }
  }

}