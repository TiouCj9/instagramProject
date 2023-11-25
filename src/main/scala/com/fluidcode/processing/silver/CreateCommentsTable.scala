package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

object CreateCommentsTable {
  def getCommentsData(bronzeData: DataFrame): DataFrame = {
    val explodeGraphImages = bronzeData.select(explode(col("GraphImages")).alias("GraphImages"))
    val selectElements = explodeGraphImages.select(col("GraphImages.__typename").alias("typename"),
      col("GraphImages.comments.data").alias("comments"),
      col("GraphImages.username").alias("username"))

    val explodeData = selectElements.select(col("typename"),
      explode(col("comments")).alias("comments"),
      col("username"))

    val commentsData = explodeData.select(col("typename"),
      col("comments.created_at").alias("comment_created_at"),
      col("comments.id").alias("comment_id"),
      col("comments.owner.id").alias("owner_id"),
      col("comments.owner.username").alias("owner_username"),
      col("comments.text").alias("comment_text"),
      col("username"))

    commentsData
  }
  def createCommentsTable(conf: Configuration, spark: SparkSession): Unit = {
    val bronzeData = spark.readStream.format("delta")
      .load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
    val postsInfo = getCommentsData(bronzeData)

    postsInfo.writeStream
      .option("checkpointLocation", s"${conf.checkpointDir}/${conf.commentsTable}")
      .format("delta")
      .outputMode("append")
      .start(s"${conf.rootPath}/${conf.database}/${conf.commentsTable}")
  }
}