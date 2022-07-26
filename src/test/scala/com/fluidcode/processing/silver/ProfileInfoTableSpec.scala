package com.fluidcode.processing.silver

import com.fluidcode.configuration.Configuration
import com.fluidcode.processing.bronze.BronzeLayer.createBronzeTable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.fluidcode.processing.silver.ProfileInfoTable._
import org.apache.spark.sql.functions.col

class ProfileInfoTableSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession  {

  test("createProfileInfoTable should create comments table from Bronze layer" ) {
    withTempDir { dir =>
      val sparkSession = spark
      import sparkSession.implicits._
      val conf = Configuration(dir.toString)
      conf.init(spark)   // creation des tables
      val path = "phil.coutinho-1-test.json"
      createBronzeTable(conf, sparkSession, path)
      Thread.sleep(5000)
      createProfileInfoTable(sparkSession, conf)
      Thread.sleep(5000)


      val result = spark.read.format("delta").load(s"${conf.rootPath}/${conf.database}/${conf.profileInfoTable}")
      val rawData = spark.read
        .option("multiLine", true)
        .json(path)
        .select(
          col("GraphProfileInfo.created_time").as("created_time").cast("Long"),
          col("GraphProfileInfo.info.biography").as("biography").cast("String"),
          col("GraphProfileInfo.info.followers_count").as("followers_count").cast("Long"),
          col("GraphProfileInfo.info.following_count").as("following_count").cast("Long"),
          col("GraphProfileInfo.info.full_name").as("full_name").cast("String"),
          col("GraphProfileInfo.info.id").as("id").cast("String"),
          col("GraphProfileInfo.info.is_business_account").as("is_business_account").cast("Boolean"),
          col("GraphProfileInfo.info.is_joined_recently").as("is_joined_recently").cast("Boolean"),
          col("GraphProfileInfo.info.is_private").as("is_private").cast("Boolean"),
          col("GraphProfileInfo.info.posts_count").as("posts_count").cast("Long"),
          col("GraphProfileInfo.info.profile_pic_url").as("profile_pic_url").cast("String"),
          col("GraphProfileInfo.username").as("username").cast("String")
        )
      assert(result.except(rawData).isEmpty)
    }
  }
}