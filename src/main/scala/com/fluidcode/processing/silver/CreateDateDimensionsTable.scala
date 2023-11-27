package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDateDimensionsTable {
  def getDateDimensions(bronzeData: DataFrame): DataFrame = {
    val explodedGraphImages = bronzeData.select(
      explode(col("GraphImages")).alias("GraphImages"),
      col("GraphProfileInfo.created_time").alias("profileCreationTime")
    )

    val explodedCommentsData = explodedGraphImages.select(
      explode(col("GraphImages.comments.data")).alias(("commentsData")),
      col("GraphImages.taken_at_timestamp").alias("postCreationTime"),
      col("profileCreationTime")
    )

    val dateDimensions = explodedCommentsData.select(
      col("commentsData.created_at").alias("commentsCreationTime"),
      col("postCreationTime"),
      col("profileCreationTime")
    )

    dateDimensions
  }
  def createDateDimensionsTable(conf: Configuration, spark: SparkSession): Unit = {
    val bronzeData = spark.readStream.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
    val dataDimensions = getDateDimensions(bronzeData)

    dataDimensions.writeStream
      .option("checkpointLocation", s"${conf.checkpointDir}/${conf.dateDimensionsTable}")
      .format("delta")
      .outputMode("append")
      .start(s"${conf.rootPath}/${conf.database}/${conf.dateDimensionsTable}")
  }
}