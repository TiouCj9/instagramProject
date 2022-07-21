package com.fluidcode.processing.silver

import com.fluidcode.models._
import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object PostInfoTable {
  def createPostInfoTable(spark: SparkSession, conf: Configuration): Unit = {

    val postInfoTable = spark.readStream
      .format ("delta")
      .load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
      .select(
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

      postInfoTable.writeStream
      .format("delta")
      .trigger (conf.trigger)
        .option( "checkpointlocation" , s"${conf.checkpointDir.toString}/${conf.postInfoTable}")
        .start(s"${conf.rootPath}/${conf.database}/${conf.postInfoTable}")
  }
}