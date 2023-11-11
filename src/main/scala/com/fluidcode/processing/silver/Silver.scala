package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

object Silver {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder()
      .appName("instagram_pipeline")
      .getOrCreate()

    val conf = Configuration(args(0))
    conf.init(spark)

    val bronzeTablePath = s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}"
    val silverInput = spark.read.format("delta").load(bronzeTablePath)
    val postInfoTable = new GetPostsInfo(silverInput, conf)
    postInfoTable.createPostsInfoTable()

  }
}
