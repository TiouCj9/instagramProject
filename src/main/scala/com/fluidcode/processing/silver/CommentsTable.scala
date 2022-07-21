package com.fluidcode.processing.silver

import com.fluidcode.models._
import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object CommentsTable {
  def CreateCommentsTable(spark: SparkSession, conf: Configuration): Unit = {

    val commentsTable = spark.readStream
      .format ("delta")
      .load(s"${conf.rootPath}/${conf.database}${conf.bronzeTable}/")
      .select(
        col("typename").cast("String"),
        col("data.created_at").alias("created_at").cast("Long"),
        col("data.id").alias("id").cast("String"),
        col("data.owner.id").alias("owner_id").cast("String"),
        col("data.owner.profile_pic_url").alias("owner_profile_pic_url").cast("String"),
        col("data.owner.username").alias("owner_username").cast("String"),
        col("data.text").alias("text").cast("String")
      )

    commentsTable.writeStream
    .format("delta")
    .trigger (conf.trigger)
    .start(s"${conf.rootPath}/${conf.database}/${conf.commentsTable}")
  }
}