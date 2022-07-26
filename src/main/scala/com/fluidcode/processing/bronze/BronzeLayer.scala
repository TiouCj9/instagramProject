package com.fluidcode.processing.bronze

import com.fluidcode.configuration.Configuration
import com.fluidcode.models.RawData
import org.apache.spark.sql.{Encoders, SparkSession}

object BronzeLayer {

  def createBronzeTable(conf: Configuration, spark:SparkSession, path: String): Unit = {
    val schema = Encoders.product[RawData].schema
    val bronzeData = spark.readStream.schema(schema).json(path)

    bronzeData
      .writeStream
      .format ("delta")
      .trigger(conf.trigger)
      .option( "checkpointlocation" , s"${conf.checkpointDir.toString}/${conf.bronzeTable}")
    .start(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
  }
}
