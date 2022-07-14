package com.fluidcode

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col , explode}

object CommentsTable {


  //todo : spark best practices
  //todo : lazy evaluation/execution (DAG)
  //TODO : fix table functions
  //TODO : fix dateDimension & tests

  def getStructGraphImages(GraphImages: DataFrame): DataFrame = {
    GraphImages.select(
      explode(col("GraphImages")).as("GraphImages")
    )
  }

  def getCommentsData(rawData: DataFrame): DataFrame = {
    getStructGraphImages(rawData).select(
      col("GraphImages.__typename").cast("String").as("typename"),
      explode(col("GraphImages.comments.data")).as("data")
    )
  }

  def getCommentsTablee(input: DataFrame) : DataFrame = {
    getCommentsData(input).select(
      col("typename").cast("String"),
      col("data.created_at").alias("created_at").cast("Long"),
      col("data.id").alias("id").cast("String"),
      col("data.owner.id").alias("owner_id").cast("String"),
      col("data.owner.profile_pic_url").alias("owner_profile_pic_url").cast("String"),
      col("data.owner.username").alias("owner_username").cast("String"),
      col("data.text").alias("text").cast("String")
    )
  }
}
