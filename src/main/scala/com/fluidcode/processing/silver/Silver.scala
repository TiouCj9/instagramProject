package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.SparkSession
import com.fluidcode.processing.silver.GetPostsInfoAndCreateTable._


object Silver {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder()
      .appName("instagram_pipeline")
      .getOrCreate()

    val conf = Configuration(args(0))
    conf.init(spark)

    val bronzeTablePath = s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}/"
    val bronzeData = spark.read.format("delta").load(bronzeTablePath)
    val postsInfoData = getPostsInfo(bronzeData)
    createPostsInfoTable(postsInfoData, conf)


  }
}
