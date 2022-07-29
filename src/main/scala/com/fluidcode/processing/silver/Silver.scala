package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import com.fluidcode.processing.silver.DateDimension._
import org.apache.spark.sql.SparkSession

object Silver {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("instagram_pipeline")
      .getOrCreate()
    val conf = Configuration(args(0))
    val commentsTable = new CommentsTable(spark, conf)
    val postInfoTable = new PostInfoTable(spark, conf)
    val profileInfoTable = new ProfileInfoTable(spark, conf)

    CreateDateDimensionTable(spark, conf)
    commentsTable.CreateCommentsTable().processAllAvailable()
    profileInfoTable.createProfileInfoTable().processAllAvailable()
    postInfoTable.createPostInfoTable().processAllAvailable()
  }
}