package com.fluidcode.processing.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object CommentsTableUtils {


  //todo : spark best practices
  //todo : lazy evaluation/execution (DAG)
  //TODO : fix table functions
  //TODO : fix dateDimension & tests

  def getStructGraphImages(GraphImages: DataFrame): DataFrame = {
    GraphImages.select(
      explode(col("GraphImages")).as("GraphImages")
    )
  }

  def getCommentsData(rawData: DataFrame): DataFrame = {
    getStructGraphImages(rawData).select(
      col("GraphImages.__typename").cast("String").as("typename"),
      explode(col("GraphImages.comments.data")).as("data")
    )
  }
}
