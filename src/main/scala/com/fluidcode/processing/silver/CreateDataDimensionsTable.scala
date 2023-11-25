package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDataDimensionsTable {
  def getDataDimensions(bronzeData: DataFrame): DataFrame = {
    val explodedGraphImages = bronzeData.select(explode(col("GraphImages")).alias("GraphImages"),
      col("GraphImages.dimensions.height").alias("height"),
      col("GraphImages.dimensions.width").alias("width"))
    val dataDimensions = explodedGraphImages.select(
      col("GraphImages.dimensions.height").alias("height"),
      col("GraphImages.dimensions.width").alias("width")
    )
    dataDimensions
  }
  def createDataDimensionsTable(conf: Configuration, spark: SparkSession): Unit = {
    val bronzeData = spark.readStream.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
    val dataDimensions = getDataDimensions(bronzeData)

    dataDimensions.writeStream
      .option("checkpointLocation", s"${conf.checkpointDir}/${conf.dataDimensionsTable}")
      .format("delta")
      .outputMode("append")
      .start(s"${conf.rootPath}/${conf.database}/${conf.dataDimensionsTable}")
  }
}