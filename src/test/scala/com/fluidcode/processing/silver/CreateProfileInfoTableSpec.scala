package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import com.fluidcode.models.bronze._
import com.fluidcode.models.silver.SilverProfileInfo
import com.fluidcode.processing.bronze.BronzeLayer
import com.fluidcode.processing.silver.CreateProfileInfoTable._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession

class CreateProfileInfoTableSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession {

  test("getProfileInfo should select profile info data from Bronze Layer" ) {

    val sparkSession = spark
    import sparkSession.implicits._
    spark.conf.set(
      "spark.sql.optimizer.excludedRules",
      "org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin"
    )

    val sampleDf = Seq(
      Data(Array(GraphImagesElements("Graph1", CommentsData(Array(DataElements(1623779105, "1382894360", OwnerData("138289436000", "https://profile_pic_url1", "mehrez"), "comment_text1"))),
        comments_disabled = true, DimensionsData(1080, 1920), "https://instagram.url1", Likes(100), Captions(Array(EdgesElements(NodeData("caption_text1")))),
        Comments(50), null, "image_id1", is_video = false, null, "s564gsd", Owner("138289436"), "shortcode1", Array("tag1"),
        1623779104, Array(ThumbnailElements(150, 150, "https://instagramthumbnail_src1")), "https://thumbnail_src1", Array("url1", "url2"),
        "benz")),
        ProfileInfo(1623779107, Info("biography1", 1000, 500, "full_name1", "138289cc", is_business_account = true, is_joined_recently = false, is_private = false, 100,
          "https://profile_pic_url1"), "benz"))
    ).toDF()

    val result = getProfileInfo(sampleDf)

    val expectedResult = Seq(
      SilverProfileInfo(1623779107, "biography1", 1000, 500, "full_name1", "138289cc", is_business_account = true,
        is_joined_recently = false, is_private = false, 100, "benz")
    ).toDF()

    assert(result.except(expectedResult).isEmpty)
  }

  test("createProfileInfoTable should create profile info table from Bronze Layer" ) {
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

      createProfileInfoTable(conf, spark)
      Thread.sleep(5000)

      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.profileInfoTable}")
      val expectedResult = Seq(
        SilverProfileInfo(1623779107, "biography1", 1000, 500, "full_name1", "138289cc", is_business_account = true,
          is_joined_recently = false, is_private = false, 100, "benz")
      ).toDF()
      assert(result.except(expectedResult).isEmpty)
    }
  }
}