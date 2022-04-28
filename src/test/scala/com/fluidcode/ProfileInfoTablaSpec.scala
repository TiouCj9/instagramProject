package com.fluidcode

import com.fluidcode.ProfileInfoTable._
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


case class Profile(created_time: Long, biography: String, info_followers_count: Long, info_following_count: Long, info_full_name: String,
                   info_id: String, info_is_business_account: Boolean, info_is_joined_recently: Boolean, info_is_private: Boolean, info_posts_count: Long,
                   profile_pic_url: String, username: String)
case class RawData(created_time: Long, info: Data , username: String)
case class Data (biography: String, followers_count: Long, following_count: Long, full_name: String, id: String, is_business_account: Boolean, is_joined_recently: Boolean,
                 is_private: Boolean, posts_count: Long, profile_pic_url: String)
case class GraphProfileInfoData (GraphProfileInfo: RawData)

class ProfileInfoTable extends AnyFlatSpec with Matchers with GivenWhenThen {
  "getProfileInfoTable" should "extract comments data from raw data" in {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("flattenDataFrame_Test")
      .getOrCreate()
    import spark.implicits._

    Given("the raw data")
    val input = Seq(GraphProfileInfoData(RawData(1286323200,info = Data("",23156762,1092,"Philippe Coutinho","1382894360",false,false,false,618,
    "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/69437559_363974237877617_991135940606951424_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=uiYY_up9lLwAX8rG9wR&edm=ABfd0MgBAAAA&ccb=7-4&oh=c3a24d2609c83e4cf8d017318f3b034e&oe=60CBC5C0&_nc_sid=7bff83"),"phil.coutinho")))
    .toDF()

    When("getProfileInfo Is invoked")
    val ProfileTable = getProfileInfoTable(input)

    Then("ProfileInfo should contain the same element as raw data")
    val expectedResult = Seq(
      Profile(1286323200,"",23156762,1092,"Philippe Coutinho","1382894360",false,false,false,618,"https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/69437559_363974237877617_991135940606951424_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=uiYY_up9lLwAX8rG9wR&edm=ABfd0MgBAAAA&ccb=7-4&oh=c3a24d2609c83e4cf8d017318f3b034e&oe=60CBC5C0&_nc_sid=7bff83","phil.coutinho")
    ).toDF()
    expectedResult.collect() should contain theSameElementsAs(ProfileTable.collect())
  }
}
