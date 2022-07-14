package com.fluidcode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object PostInfoTable {

  def getStructGraphImages(GraphImage: DataFrame): DataFrame = {
    GraphImage.select(
      explode(col("GraphImages")).as("GraphImages")
    )
  }
  def getCommentsInfoData(RawData: DataFrame): DataFrame = {
    getStructGraphImages(RawData).select(
      col("GraphImages").getField("comments_disabled").alias("comments_disabled").cast("Boolean"),
      col("GraphImages").getField("dimensions").getField("height").alias("dimensions_height").cast("Long"),
      col("GraphImages").getField("dimensions").getField("width").alias("dimensions_width").cast("Long"),
      col("GraphImages").getField("display_url").alias("display_url").cast("String"),
      col("GraphImages").getField("edge_media_preview_like").getField("count").alias("edge_media_preview_like_count").cast("Long"),
      col("GraphImages").getField("edge_media_to_comment").getField("count").alias("edge_media_to_comment_count").cast("Long"),
      col("GraphImages").getField("gating_info").alias("gating_info").cast("String"),
      col("GraphImages").getField("id").alias("id").cast("String"),
      col("GraphImages").getField("is_video").alias("is_video").cast("Boolean"),
      col("GraphImages").getField("location").alias("location").cast("String"),
      col("GraphImages").getField("media_preview").alias("media_preview").cast("String"),
      col("GraphImages").getField("owner").getField("id").alias("owner_id").cast("String"),
      col("GraphImages").getField("shortcode").alias("shortcode").cast("String"),
      col("GraphImages").getField("taken_at_timestamp").alias("taken_at_timestamp").cast("Long"),
      col("GraphImages").getField("thumbnail_src").alias("thumbnail_src").cast("String"),
      col("GraphImages").getField("username").alias("username").cast("String"),
      col("GraphImages").getField("thumbnail_resources").alias("thumbnail_resources"),
      col("GraphImages").getField("urls").alias("urls"),
      col("GraphImages").getField("tags").alias("tags"),
      (explode(col("GraphImages").getField("edge_media_to_caption").getField("edges")).as("edges"))

    )
  }
  def getEdgeMediaCaptionData(Datas: DataFrame): DataFrame = {
    getCommentsInfoData(Datas).select( //select all the above + select structs from exploded column (example : edges)
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
      col("edges").getField("node").getField("text").as("text").cast("String"),
      explode(col("thumbnail_resources")).as("thumbnail_resources")

      // explode one of the remaining arrays
    )
  }
  def getThumbnail(thumbn: DataFrame): DataFrame ={ //loop
    getEdgeMediaCaptionData(thumbn).select(
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
      col("thumbnail_resources").getField("config_height").alias("config_height").as("thumbnail_resources_config_height").cast("Long"),
      col("thumbnail_resources").getField("config_width").alias("config_width").as("thumbnail_resources_config_width").cast("Long"),
      col("thumbnail_resources").getField("src").alias("src").as("thumbnail_resources_config_src").cast("String")
    )
  }


  def getPostInfoTable(Raw: DataFrame) : DataFrame = {
    getThumbnail(Raw).select(
      col("comments_disabled"),
      col("dimensions_height"),
      col("dimensions_width"),
      col("display_url"),
      col("edge_media_preview_like_count"),
      col("text"),
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
      col("thumbnail_resources_config_src"),
      col("urls"),
      col("username")
    )
  }

}

