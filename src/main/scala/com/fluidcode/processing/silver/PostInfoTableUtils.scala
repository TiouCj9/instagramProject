package com.fluidcode.processing.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object PostInfoTableUtils {

  def getStructGraphImages(RawData: DataFrame): DataFrame = {
    RawData.select(
      explode(col("GraphImages")).as("GraphImages")
    )
  }
  def getCommentsInfoData(data: DataFrame): DataFrame = {
    getStructGraphImages(data).select(
      col("GraphImages.comments_disabled").alias("comments_disabled"),
      col("GraphImages.dimensions.height").alias("dimensions_height"),
      col("GraphImages.dimensions.width").alias("dimensions_width"),
      col("GraphImages.display_url").alias("display_url"),
      col("GraphImages.edge_media_preview_like.count").alias("edge_media_preview_like_count"),
      col("GraphImages.edge_media_to_comment.count").alias("edge_media_to_comment_count"),
      col("GraphImages.gating_info").alias("gating_info"),
      col("GraphImages.id").alias("id"),
      col("GraphImages.is_video").alias("is_video"),
      col("GraphImages.location").alias("location"),
      col("GraphImages.media_preview").alias("media_preview"),
      col("GraphImages.owner.id").alias("owner_id"),
      col("GraphImages.shortcode").alias("shortcode"),
      col("GraphImages.taken_at_timestamp").alias("taken_at_timestamp"),
      col("GraphImages.thumbnail_src").alias("thumbnail_src"),
      col("GraphImages.username").alias("username"),
      col("GraphImages.thumbnail_resources").alias("thumbnail_resources"),
      col("GraphImages.urls").alias("urls"),
      col("GraphImages.tags").alias("tags"),
      explode(col("GraphImages.edge_media_to_caption.edges")).as("edges")

    )
  }
  def getEdgeMediaCaptionData(Datas: DataFrame): DataFrame = {
    getCommentsInfoData(Datas).select(
      col("comments_disabled"),
      col("dimensions_height"),
      col("dimensions_width"),
      col("display_url"),
      col("edge_media_preview_like_count"),
      col("edge_media_to_comment_count"),
      col("gating_info"),
      col("id"),
      col("is_video"),
      col("location"),
      col("media_preview"),
      col("owner_id"),
      col("shortcode"),
      col("taken_at_timestamp"),
      col("tags"),
      col("urls"),
      col("username"),
      col("edges.node.text").as("text").cast("String"),
      explode(col("thumbnail_resources")).as("thumbnail_resources")
    )
  }

  def getPostInfoData(Datum: DataFrame): DataFrame ={
    getEdgeMediaCaptionData(Datum).select(
      col("comments_disabled"),
      col("dimensions_height"),
      col("dimensions_width"),
      col("display_url"),
      col("edge_media_preview_like_count"),
      col("edge_media_to_comment_count"),
      col("gating_info"),
      col("id"),
      col("is_video"),
      col("location"),
      col("media_preview"),
      col("owner_id"),
      col("shortcode"),
      col("tags"),
      col("text"),
      col("urls"),
      col("taken_at_timestamp"),
      col("username"),
      col("thumbnail_resources.config_height").alias("config_height").as("thumbnail_resources_config_height").cast("Long"),
      col("thumbnail_resources.config_width").alias("config_width").as("thumbnail_resources_config_width").cast("Long"),
      col("thumbnail_resources.src").alias("src").as("thumbnail_resources_config_src").cast("String"),
      col("urls"),
      col("username")
    )
  }




}
