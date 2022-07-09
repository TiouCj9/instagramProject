package com.fluidcode

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ProfileInfoTable {

  def getProfileInfoTable(RawData: DataFrame): DataFrame = {
    RawData.select(
      col("GraphProfileInfo").getField("created_time").as("created_time").cast("Long"),
      col("GraphProfileInfo").getField("info").getField("biography").as("biography").cast("String"),
      col("GraphProfileInfo").getField("info").getField("followers_count").as("followers_count").cast("Long"),
      col("GraphProfileInfo").getField("info").getField("following_count").as("following_count").cast("Long"),
      col("GraphProfileInfo").getField("info").getField("full_name").as("full_name").cast("String"),
      col("GraphProfileInfo").getField("info").getField("id").as("id").cast("String"),
      col("GraphProfileInfo").getField("info").getField("is_business_account").as("is_business_account").cast("Boolean"),
      col("GraphProfileInfo").getField("info").getField("is_joined_recently").as("is_joined_recently").cast("Boolean"),
      col("GraphProfileInfo").getField("info").getField("is_private").as("is_private").cast("Boolean"),
      col("GraphProfileInfo").getField("info").getField("posts_count").as("posts_count").cast("Long"),
      col("GraphProfileInfo").getField("info").getField("profile_pic_url").as("profile_pic_url").cast("String"),
      col("GraphProfileInfo").getField("username").as("username").cast("String")
    )
  }
}
