package com.fluidcode.processing.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ProfileInfoTable {

  def getProfileInfoTable(RawData: DataFrame): DataFrame = {
    RawData.select(
      col("GraphProfileInfo.created_time").as("created_time").cast("Long"),
      col("GraphProfileInfo.info.biography").as("biography").cast("String"),
      col("GraphProfileInfo.info.followers_count").as("followers_count").cast("Long"),
      col("GraphProfileInfo.info.following_count").as("following_count").cast("Long"),
      col("GraphProfileInfo.info.full_name").as("full_name").cast("String"),
      col("GraphProfileInfo.info.id").as("id").cast("String"),
      col("GraphProfileInfo.info.is_business_account").as("is_business_account").cast("Boolean"),
      col("GraphProfileInfo.info.is_joined_recently").as("is_joined_recently").cast("Boolean"),
      col("GraphProfileInfo.info.is_private").as("is_private").cast("Boolean"),
      col("GraphProfileInfo.info.posts_count").as("posts_count").cast("Long"),
      col("GraphProfileInfo.info.profile_pic_url").as("profile_pic_url").cast("String"),
      col("GraphProfileInfo.username").as("username").cast("String")
    )
  }
}
