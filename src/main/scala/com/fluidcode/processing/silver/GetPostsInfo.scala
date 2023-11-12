package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

class GetPostsInfo(bronzeData: DataFrame, conf: Configuration) {
  def createPostsInfoTable(): Unit = {
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
      col("edge_media_to_comment_count"),
      col("gating_info"),
      col("id"),
      col("is_video"),
      col("location"),
      col("owner_id"),
      col("shortcode"),
      col("tags"),
      col("taken_at_timestamp"),
      col("username"),
      explode(col("edge_media_to_caption_edges.node.text")).alias("edge_media_to_caption_edges_text")
    )
    postsInfo.write.format("delta").mode("overwrite")
      .option("path", s"${conf.rootPath}.${conf.database}.${conf.postInfoTable}")
      .saveAsTable(s"${conf.postInfoTable}")
  }
}
