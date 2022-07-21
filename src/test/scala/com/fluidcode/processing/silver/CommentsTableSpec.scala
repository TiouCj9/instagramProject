package com.fluidcode.processing.silver

import com.fluidcode.models._
import com.fluidcode.processing.silver.CommentsTableUtils._
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommentsTableSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  "getCommentsTable" should "extract comments data from raw data" in {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("flattenDataFrame_Test")
      .getOrCreate()
    import spark.implicits._


    Given("the raw data")
    val rawData = Seq(graphImagees(Array(GraphImagesData(__typename = "GraphImage", comments = Comment(data = Array(Datum(created_at = 1617213018,id = "17848025879550357",owner = Owner(id = "11357305166", profile_pic_url = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/199980176_874196559834598_6114321173318074396_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=KcNRsr4RA5YAX8aN0h2&edm=AI-cjbYBAAAA&ccb=7-4&oh=d2365d2e541dac89ae18dc334cec4962&oe=60CAF207&_nc_sid=ba0005",username = "proudlycouto"),text = "congratulation"))))))).toDF()

    When("CommentsTable Is invoked")
    val commentsTable = getCommentsData(rawData)
    Then("CommentTable should contain the same element as raw data")
    val expectedResult = Seq(
      Comments(typename = "GraphImage",created_at = 1617213018, id = "17848025879550357", owner_id = "11357305166", owner_profile_pic_url = "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/199980176_874196559834598_6114321173318074396_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=KcNRsr4RA5YAX8aN0h2&edm=AI-cjbYBAAAA&ccb=7-4&oh=d2365d2e541dac89ae18dc334cec4962&oe=60CAF207&_nc_sid=ba0005",owner_username = "proudlycouto", text = "congratulation")).toDF()
    expectedResult.collect() should contain theSameElementsAs(commentsTable.collect())

  }
}