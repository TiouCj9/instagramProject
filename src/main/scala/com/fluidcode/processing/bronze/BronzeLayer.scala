package com.fluidcode.processing.bronze

import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.{Encoders, SparkSession}

class BronzeLayer(conf: Configuration, spark: SparkSession, path: String) {

  def createBronzeTable(): Unit = {

    val bronzeData = spark.readStream.option("multiLine", true).json(path)

    bronzeData
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("path", s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
      .start()
  }
}
