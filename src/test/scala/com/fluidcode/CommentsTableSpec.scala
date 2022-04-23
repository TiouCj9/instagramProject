package com.fluidcode

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.fluidcode.CommentsTable._
case class Comments(
                     typename: String,
                     created_at: Long,
                     id: String,
                     owner_id: String,
                     owner_profile_pic_url: String,
                     owner_username: String,
                     text: String)
case class GraphImagesData(__typename : String, comments : Comment)
case class GraphImages(GraphImages: Array[GraphImagesData])
case class Comment (data: Array[Data])
case class Data (created_at: Long, id: String, owner: Owner, text: String)
case class Owner(id: String, profile_pic_url: String, username: String)

class CommentsTableSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  "getCommentsTable" should "extract comments data from raw data" in {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("flattenDataFrame_Test")
      .getOrCreate()
    import spark.implicits._


    Given("the raw data")
    val rawData = Seq(GraphImages(Array(GraphImagesData(__typename = "GraphImage",
      comments =  Comment(
        Array(
          Data(
            created_at = 1619023963,
            id = "18209883163069294",
            owner = Owner(
              id = "20740995",
              profile_pic_url = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/41610857_333107223920170_9061775206602768384_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=fUUiX6p6YAoAX9hlHM5&edm=AI-cjbYBAAAA&ccb=7-4&oh=7f3972522d18f4f30943b49656f57b81&oe=60CA67BD&_nc_sid=ba0005",
              username = "sergiroberto"),
            text = "💪🏼💪🏼"),
          Data(
            created_at = 1619023981,
            id = "18114517408211027",
            owner = Owner(
              id = "268668518",
              profile_pic_url = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/145618949_157498136023202_8971597364344501743_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=Kb6K0HyxiwgAX_xz_8v&edm=AI-cjbYBAAAA&ccb=7-4&oh=21bd0fcf2b127b5d38787edb2892715a&oe=60CC0D4B&_nc_sid=ba0005",
              username = "juliana_gilaberte"),
            text = "🙏🏻 Deus não erra, não falha, Ele sabe de todas as coisas! 🙌🏻 Deus está no comando da sua vida e logo vc estará de volta aos campos com força total 🦵🏻 ⚽️ 🥅"
          )

        )))))).toDF()

    When("CommentsTable Is invoked")
    val commentsTable = getCommentsTable(rawData)
    Then("CommentTable should contain the same element as raw data")
    val expectedResult = Seq(
      Comments("GraphImage" ,1619023963, "18209883163069294", "20740995", "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/41610857_333107223920170_9061775206602768384_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=fUUiX6p6YAoAX9hlHM5&edm=AI-cjbYBAAAA&ccb=7-4&oh=7f3972522d18f4f30943b49656f57b81&oe=60CA67BD&_nc_sid=ba0005", "sergiroberto", "💪🏼💪🏼"),
      Comments("GraphImage" ,1619023981, "18114517408211027", "268668518", "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/145618949_157498136023202_8971597364344501743_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=Kb6K0HyxiwgAX_xz_8v&edm=AI-cjbYBAAAA&ccb=7-4&oh=21bd0fcf2b127b5d38787edb2892715a&oe=60CC0D4B&_nc_sid=ba0005", "juliana_gilaberte", "🙏🏻 Deus não erra, não falha, Ele sabe de todas as coisas! 🙌🏻 Deus está no comando da sua vida e logo vc estará de volta aos campos com força total 🦵🏻 ⚽️ 🥅")
    ).toDF()
    expectedResult.as[Comments].collect() should contain theSameElementsAs(commentsTable.as[Comments].collect())
  }
}