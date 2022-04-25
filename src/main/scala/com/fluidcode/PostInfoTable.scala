package com.fluidcode

import com.fluidcode.CommentsTable.getCommentsData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object PostInfoTable {

  def getStructGraphImages(GraphImages: DataFrame): DataFrame = {
    GraphImages.select(
      explode(col("GraphImages")).as("GraphImages")
    )
  }
  def getCommentsInfoData(RawData: DataFrame): DataFrame={
    getStructGraphImages(RawData).select(
      col("GraphImages").getField("comments_disabled").alias("comments_disabled").cast("Boolean"),
      col("GraphImages").getField("dimensions").getField("height").alias("dimensions_height").cast("Long"),
      col("GraphImages").getField("dimensions").getField("width").alias("dimensions_width").cast("Long"),
      col("GraphImages").getField("display_url").alias("display_url").cast("String"),
      col("GraphImages").getField("edge_media_preview_like").getField("count").alias("edge_media_preview_like_count").cast("Long"),
      explode(col("GraphImages.edge_media_to_caption.edges").as("edge_media_to_caption_edges"))
    )
  }


  def getEdgeMediaCaptionData(rawData: DataFrame): DataFrame = {
    getCommentsInfoData(rawData).select(
      col("edge_media_to_caption_edges").getField("node").getField("count").alias("edge_media_to_caption_edges_node_text").cast("String"),
      col("GraphImages").getField("edge_media_to_comment").getField("count").alias("edge_media_to_comment_count").cast("Long"),
      col("GraphImages").getField("gating_info").alias("gating_info").cast("String"),
      col("GraphImages").getField("id").alias("id").cast("String"),
      col("GraphImages").getField("is_video").alias("is_video").cast("Boolean"),
      col("GraphImages").getField("location").alias("location").cast("String"),
      col("GraphImages").getField("media_preview").alias("media_preview").cast("String"),
      col("GraphImages").getField("owner").getField("id").alias("owner_id").cast("String"),
      col("GraphImages").getField("shortcode").alias("shortcode").cast("String"),
      explode(col("GraphImages.tags")).as("tags")
    )
  }

  def getThumbnail(thumbn: DataFrame): DataFrame ={
    getEdgeMediaCaptionData(thumbn).select(
      col("tags").alias("tags").cast("String"),
      col("GraphImages").getField("taken_at_timestamp").alias("taken_at_timestamp").cast("Long"),
      explode(col("GraphImages.thumbnail_resources")).as("thumbnail_resources")
    )
  }
  def getUrlsData(Urls: DataFrame): DataFrame ={
    getThumbnail(Urls).select(
      col("thumbnail_resources").getField("config_height").alias("thumbnail_resources_config_height").cast("Long"),
      col("thumbnail_resources").getField("config_width").alias("thumbnail_resources_config_width").cast("Long"),
      col("thumbnail_resources").getField("src").alias("thumbnail_resources_src").cast("String"),
      explode(col("GraphImages.urls")).as("urls")
    )
  }

  def getPostInfoTable(Raw: DataFrame) : DataFrame = {
    getUrlsData(Raw).select(
      col("comments_disabled"),
      col("dimensions_height"),
      col("dimensions_width"),
      col("display_url"),
      col("edge_media_preview_like_count"),
      col("edge_media_to_caption_edges_node_text"),
      col("edge_media_to_comment_count"),
      col("gating_info"),
      col("id"),
      col("is_video"),
      col("location"),
      col("media_preview"),
      col("owner_id"),
      col("shortcode"),
      col("tags"),
      col("taken_at_timestamp"),
      col("thumbnail_resources_config_height"),
      col("thumbnail_resources_config_width"),
      col("thumbnail_resources_src"),
      col("urls").alias("urls").cast("String"),
      col("GraphImages").getField("username").alias("username").cast("String")
    )
  }

}

