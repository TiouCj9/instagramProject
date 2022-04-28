package com.fluidcode
import com.fluidcode.PostInfoTable._
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


  case class GraphImages(GraphImages: Array[PostInfoData])
  case class PostInfoData(comments_disabled: Boolean, dimensions: Dimensions,
                           display_url: String, edge_media_preview_like: Edge_media_preview_like, edge_media_to_caption: edge_media_to_caption,
                           edge_media_to_comment:Edge_media_to_comment, gating_info: String, id: String, is_video: Boolean, location: String,
                           media_preview: String, owner:Owners, shortcode: String, tags: Tags, taken_at_timestamp: Long, thumbnail_resources:Thumbnail_Resources,
                           thumbnail_src: String, urls:Urls, username: String)
  case class Dimensions(height: Long, width: Long)
  case class Edge_media_preview_like(count: Long)
  case class edge_media_to_caption(edges: Array[edgesData])
  case class edgesData(node: node)
  case class node(text: String)
  case class Edge_media_to_comment(count: Long)
  case class Owners(id: String)
  case class Tags(tags: Array[String])
  case class Urls(urls: Array[String])
  case class Thumbnail_Resources(thumbnail_Resources: Array[Thumbnail_ResourcesData])
  case class Thumbnail_ResourcesData(config_height: Long, config_width: Long, src:String)
  case class PostInfoS(comments_disabled: Boolean, dimensions_height: Long, dimensions_width: Long, display_url: String, edge_media_preview_like_count: Long,
                       edge_media_to_caption_edges_node_text:String, edge_media_to_comment_count: Long, gating_info: String, id: String, is_video: Boolean,
                       location: String, media_preview: String,  owner_id: String, shortcode: String, tags: Tags, taken_at_timestamp: Long, thumbnail_resources_config_height: Long,
                       thumbnail_resources_config_width: Long, thumbnail_resources_config_src: String  ,thumbnail_src: String, urls: Urls, username: String)

  class PostInfoTableSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
    "getPostInfoTable" should "extract comments data from raw data" in {
      val spark: SparkSession = SparkSession
        .builder()
        .master("local[*]")
        .appName("flattenDataFrame_Test")
        .getOrCreate()
      import spark.implicits._

      Given("the raw data")
      val RawData =  Seq(GraphImages(Array(PostInfoData(comments_disabled = false, dimensions = Dimensions(720, 1080), display_url = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=6610cca9bb632966cade9c27cdfd3bf5&oe=60CBE492&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2-ccb7-4",
                          edge_media_preview_like = Edge_media_preview_like(692580), edge_media_to_caption = edge_media_to_caption(Array(edgesData(node = node(text = "L🔴✔️E  YOU ♥️#myvalentineforlife😍#myqueen")))),
                          edge_media_to_comment = Edge_media_to_comment (31), "null", "2509090007038408221", false, "null", "null", owner = Owners(id = "1382894360"),"CLSFBFTAbYd", tags = Tags(Array("myqueen","myvalentineforlife")),
                          1613326858, thumbnail_resources = Thumbnail_Resources(Array(Thumbnail_ResourcesData(config_height = 150, 150, "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/c240.0.960.960a/s150x150/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=1ca74791d9abaa88d6159228198157e0&oe=60CAC323&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2.c-ccb7-4"))), "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/sh0.08/e35/c240.0.960.960a/s640x640/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=0824aeded92582ee00295d07f98e636f&oe=60CAF7CB&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2.c-ccb7-4",
                          urls = Urls(Array("https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=AABBvjUBAAAA&ccb=7-4&oh=ca79dd9a231700ef722ff531cea4dc60&oe=60CBE492&_nc_sid=83d603&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2-ccb7-4",
                          "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/150567891_438748344236267_8358558477012118456_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=111&_nc_ohc=afZ27aj0aXwAX9ODWZ7&edm=AABBvjUBAAAA&ccb=7-4&oh=99c6d520cf8d349249fa561e6917991a&oe=60CB4C2E&_nc_sid=83d603&ig_cache_key=MjUwOTA5MDAwMjQwODAyNDIxOQ%3D%3D.2-ccb7-4")),
                          "phil.coutinho" )))).toDF()

      When("getPostInfoTable Is invoked")
      val PostInfoTable = getPostInfoTable(RawData)

      Then("PostPostTable should contain the same element as raw data")
      val expectedResult = Seq(PostInfoS(false, 720, 1080, "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=6610cca9bb632966cade9c27cdfd3bf5&oe=60CBE492&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2-ccb7-4",
                           692580, "L🔴✔️E  YOU ♥️#myvalentineforlife😍#myqueen", 31, "null", "2509090007038408221", false, "null", "null", "1382894360", "CLSFBFTAbYd",
                           tags = Tags(Array("myqueen","myvalentineforlife")), 1613326858, 150, 150,
                           "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/c240.0.960.960a/s150x150/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=1ca74791d9abaa88d6159228198157e0&oe=60CAC323&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2.c-ccb7-4",
                           thumbnail_src = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/sh0.08/e35/c240.0.960.960a/s640x640/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=0824aeded92582ee00295d07f98e636f&oe=60CAF7CB&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2.c-ccb7-4",
                           urls = Urls (Array("https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=AABBvjUBAAAA&ccb=7-4&oh=ca79dd9a231700ef722ff531cea4dc60&oe=60CBE492&_nc_sid=83d603&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2-ccb7-4","https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/150567891_438748344236267_8358558477012118456_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=111&_nc_ohc=afZ27aj0aXwAX9ODWZ7&edm=AABBvjUBAAAA&ccb=7-4&oh=99c6d520cf8d349249fa561e6917991a&oe=60CB4C2E&_nc_sid=83d603&ig_cache_key=MjUwOTA5MDAwMjQwODAyNDIxOQ%3D%3D.2-ccb7-4")),
                           username = "phil.coutinho"
                           )).toDF()
      expectedResult.collect() should contain theSameElementsAs(PostInfoTable.collect())
    }
}
