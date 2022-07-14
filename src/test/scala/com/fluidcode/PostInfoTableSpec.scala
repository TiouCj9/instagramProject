package com.fluidcode
import com.fluidcode.PostInfoTable._
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions.col

  class PostInfoTableSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
    "getPostInfoTable" should "extract comments data from raw data" in {
      val spark: SparkSession = SparkSession
        .builder()
        .master("local[*]")
        .appName("flattenDataFrame_Test")
        .getOrCreate()
      import spark.implicits._

      Given("the raw data")
      val RawDataA = Seq(GraphImages
      (Array
        (PostInfoData(
          comments_disabled = false, dimensions = Dimensions(height = 720, width = 1080),display_url = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=6610cca9bb632966cade9c27cdfd3bf5&oe=60CBE492&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2-ccb7-4",
          edge_media_preview_like = Edge_media_preview_like (count = 692580), edge_media_to_caption = Edge_media_to_caption(edges = Array(edges(node = Node(text = "Stringfff")))),
          edge_media_to_comment = Edge_media_to_comment(count = 31), gating_info = "null", id = "2509090007038408221", is_video = false, location = "null", media_preview = "null" , owner = Owners(id = "1382894360"),
          shortcode = "CLSFBFTAbYd", tags = Array("myqueen","oo") , taken_at_timestamp = 1613326858, thumbnail_resources = Array(Thumbnail_resources(config_height = 150, config_width = 150, src = "20")) ,
          thumbnail_src = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/sh0.08/e35/c240.0.960.960a/s640x640/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=0824aeded92582ee00295d07f98e636f&oe=60CAF7CB&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2.c-ccb7-4",
          urls = Array("test","ll"),
          username = "phil.coutinho"
        )))).toDF()
      When("getPostInfoTable Is invoked")
      val PostInfoTable = getPostInfoTable(RawDataA)

      Then("PostInfoTable should contain the same element as raw data")
     val expectedResult = Seq(PostInfoS(comments_disabled = false, dimensions_height = 720, dimensions_width = 1080, display_url = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=6610cca9bb632966cade9c27cdfd3bf5&oe=60CBE492&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2-ccb7-4",
        edge_media_preview_like_count = 692580, text = "Stringfff", edge_media_to_comment_count = 31, gating_info="null", id="2509090007038408221", is_video = false, location = "null",
        media_preview = "null", owner_id = "1382894360", shortcode = "CLSFBFTAbYd", tags = Array("myqueen","oo"), taken_at_timestamp = 1613326858 , thumbnail_resources_config_height = 150, thumbnail_resources_config_width = 150, thumbnail_resources_config_src = "20",
        urls = Array("test","ll"),
        username = "phil.coutinho")).toDF()
      val emptyConf: Seq[PostInfoS] = Seq()
      expectedResult.join(PostInfoTable, PostInfoTable("comments_disabled") === expectedResult("comments_disabled") , "leftAnti").show()
      //PostInfoTable.show()
    }
}