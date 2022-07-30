package com.fluidcode.processing.bronze
import com.fluidcode.configuration.Configuration
import org.apache.spark.sql.SparkSession

object Bronze {
 def main (args: Array[String]): Unit = {
   val spark : SparkSession = SparkSession.builder()
     .appName("instagram_pipeline")
     .getOrCreate()
   val conf = Configuration(args(0))
   conf.init(spark)
   val bronzeLayer = new BronzeLayer(conf, spark, args(1))
   bronzeLayer.createBronzeTable()
 }
}
