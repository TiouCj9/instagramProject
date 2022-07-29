package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import com.fluidcode.processing.bronze.BronzeLayer
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.fluidcode.processing.silver.CommentsTableUtils.getCommentsData
import org.apache.spark.sql.functions.col

class CommentsTableSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("CreateCommentsTable should create comments table from Bronze layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      import sparkSession.implicits._
      val conf = Configuration(dir.toString)
      conf.init(spark)   // creation des tables

      val path = "phil.coutinho-1-test.json"
      val bronzeLayer = new BronzeLayer(conf, sparkSession, path)
      bronzeLayer.createBronzeTable()
      Thread.sleep(5000)
      val createCommentsTable = new CommentsTable(spark, conf)
      createCommentsTable.CreateCommentsTable().processAllAvailable()
      Thread.sleep(5000)


      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.commentsTable}")
      val rawData = spark.read
        .option("multiLine", true)
        .json(path)

        val expectedResult = getCommentsData(rawData)
      .select(
        col("typename").cast("String"),
        col("data.created_at").alias("created_at").cast("Long"),
        col("data.id").alias("id").cast("String"),
        col("data.owner.id").alias("owner_id").cast("String"),
        col("data.owner.profile_pic_url").alias("owner_profile_pic_url").cast("String"),
        col("data.owner.username").alias("owner_username").cast("String"),
        col("data.text").alias("text").cast("String")
      )
      assert(result.except(expectedResult).isEmpty)
    }
  }
}