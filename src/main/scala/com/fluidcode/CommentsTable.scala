package com.fluidcode

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col , explode}

object CommentsTable {

  //todo: running modes of spark
  //todo : dataframe vs rdd vs dataset
  //todo : narrow vs wide transformations
  //todo : action vs transformation

  def getStructGraphImages(GraphImages: DataFrame): DataFrame = {
    GraphImages.select(
      explode(col("GraphImages")).as("GraphImages")
    )
  }

  def getCommentsData(rawData: DataFrame): DataFrame = {
    getStructGraphImages(rawData).select(
      col("GraphImages").getField("__typename").alias("typename").cast("String"),
      explode(col("GraphImages.comments.data")).as("data")
    )
  }

  def getCommentsTable(input: DataFrame) : DataFrame = {
    getCommentsData(input).select(
      col("typename"),
      col("data").getField("created_at").alias("created_at").cast("Long"),
      col("data").getField("id").alias("id").cast("String"),
      col("data").getField("owner").getField("id").alias("owner_id").cast("String"),
      col("data").getField("owner").getField("profile_pic_url").alias("owner_profile_pic_url").cast("String"),
      col("data").getField("owner").getField("username").alias("owner_username").cast("String"),
      col("data").getField("text").alias("text").cast("String")
    )
  }
}
