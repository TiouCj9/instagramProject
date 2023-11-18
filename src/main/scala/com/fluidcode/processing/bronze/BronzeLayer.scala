package com.fluidcode.processing.bronze

import com.fluidcode.configuration.Configuration
import com.fluidcode.configuration.Configuration.CHECKPOINT_DIR
import com.fluidcode.models.bronze.Data
import org.apache.spark.sql.{Encoders, SparkSession}

class BronzeLayer(conf: Configuration, spark: SparkSession, path: String) {
  def createBronzeTable(): Unit = {

    val bronzeSchema = Encoders.product[Data].schema
    val bronzeData = spark.readStream.schema(bronzeSchema).json(path)

    bronzeData.writeStream
      .option("checkpointLocation", s"${conf.rootPath}/${conf.database}/$CHECKPOINT_DIR/${conf.bronzeTable}")
      .format("delta")
      .outputMode("append")
      .start(s"${conf.rootPath}/${conf.database}/${conf.bronzeTable}")
  }
}
