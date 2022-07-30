package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.fluidcode.processing.silver.CommentsTableUtils._
class CommentsTable(spark: SparkSession, conf: Configuration) {
  def CreateCommentsTable() = {

    val newArrivingData = spark.readStream
      .format ("delta")
      .load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")

      val commentsData = getCommentsData(newArrivingData)
      .select(
        col("typename").cast("String"),
        col("data.created_at").alias("created_at").cast("Long"),
        col("data.id").alias("id").cast("String"),
        col("data.owner.id").alias("owner_id").cast("String"),
        col("data.owner.profile_pic_url").alias("owner_profile_pic_url").cast("String"),
        col("data.owner.username").alias("owner_username").cast("String"),
        col("data.text").alias("text").cast("String")
      )

      .writeStream
      .format("delta")
      .trigger(conf.trigger)
      .option("checkpointlocation" , s"${conf.checkpointDir.toString}/${conf.commentsTable}")
      .start(s"${conf.rootPath}/${conf.database}/${conf.commentsTable}")
    commentsData
  }
}