package com.fluidcode.processing.silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.fluidcode.configuration.Configuration

object CreateProfileInfoTable {
  def getProfileInfo(bronzeData: DataFrame): DataFrame = {
    val profileInfo = bronzeData.select(col("GraphProfileInfo.created_time").alias("created_time"),
      col("GraphProfileInfo.info.biography").alias("biography"),
      col("GraphProfileInfo.info.followers_count").alias("followers_count"),
      col("GraphProfileInfo.info.following_count").alias("following_count"),
      col("GraphProfileInfo.info.full_name").alias("full_name"),
      col("GraphProfileInfo.info.id").alias("id"),
      col("GraphProfileInfo.info.is_business_account").alias("is_business_account"),
      col("GraphProfileInfo.info.is_joined_recently").alias("is_joined_recently"),
      col("GraphProfileInfo.info.is_private").alias("is_private"),
      col("GraphProfileInfo.info.posts_count").alias("posts_count"),
      col("GraphProfileInfo.username").alias("username"))

    profileInfo
  }
  def createProfileInfoTable(conf: Configuration, spark: SparkSession): Unit = {
    val bronzeData = spark.readStream.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
    val profileInfo = getProfileInfo(bronzeData)

    profileInfo.writeStream
      .option("checkpointLocation", s"${conf.checkpointDir}/${conf.profileInfoTable}")
      .format("delta")
      .outputMode("append")
      .start(s"${conf.rootPath}/${conf.database}/${conf.profileInfoTable}")
  }
}