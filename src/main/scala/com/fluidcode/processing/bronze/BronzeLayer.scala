package com.fluidcode.processing.bronze

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.{Encoders, SparkSession}

class BronzeLayer(conf: Configuration, spark: SparkSession, path: String) {

  def createBronzeTable(): Unit = {

    val bronzeData = spark.read.option("multiLine", true).json(path)

    bronzeData
      .write
      .format("delta")
      .mode("append")
      .save(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
  }
}
