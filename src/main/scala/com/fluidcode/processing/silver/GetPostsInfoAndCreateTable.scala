package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import com.fluidcode.configuration.Configuration.CHECKPOINT_DIR
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

object GetPostsInfoAndCreateTable {
  def getPostsInfo(bronzeData: DataFrame): DataFrame = {

    val explodedGraphImages = bronzeData.select(explode(col("GraphImages")).alias("GraphImages"))
    val postsInfoElements = explodedGraphImages.select(
      col("GraphImages.comments_disabled").alias("comments_disabled"),
      col("GraphImages.edge_media_preview_like.count").alias("edge_media_preview_like_count"),
      col("GraphImages.edge_media_to_comment.count").alias("edge_media_to_comment_count"),
      col("GraphImages.edge_media_to_caption.edges").alias("edge_media_to_caption_edges"),
      col("GraphImages.gating_info").alias("gating_info"),
      col("GraphImages.id").alias("id"),
      col("GraphImages.is_video").alias("is_video"),
      col("GraphImages.location").alias("location"),
      col("GraphImages.owner.id").alias("owner_id"),
      col("GraphImages.shortcode").alias("shortcode"),
      explode(col("GraphImages.tags")).alias("tags"),
      col("GraphImages.taken_at_timestamp").alias("taken_at_timestamp"),
      col("GraphImages.username").alias("username")
    )
    val postsInfo = postsInfoElements.select(col("comments_disabled"),
      col("edge_media_preview_like_count"),
      explode(col("edge_media_to_caption_edges.node.text")).alias("edge_media_to_caption_edges_node_text"),
      col("edge_media_to_comment_count"),
      col("gating_info"),
      col("id"),
      col("is_video"),
      col("location"),
      col("owner_id"),
      col("shortcode"),
      col("tags"),
      col("taken_at_timestamp"),
      col("username")
    )
    postsInfo
  }
  def createPostsInfoTable(conf: Configuration, spark: SparkSession): Unit = {

    val bronzeData = spark.readStream.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
    val postsInfo = getPostsInfo(bronzeData)

    postsInfo.writeStream
      .option("checkpointLocation", s"${conf.checkpointDir}/${conf.postInfoTable}")
      .format("delta")
      .outputMode("append")
      .start(s"${conf.rootPath}/${conf.database}/${conf.postInfoTable}")
  }
}

