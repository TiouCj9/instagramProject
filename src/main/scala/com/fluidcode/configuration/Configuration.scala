package com.fluidcode.configuration

import com.fluidcode.configuration.Configuration.{createTable, _}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.FileNotFoundException

import com.fluidcode.models.bronze.Data
import com.fluidcode.models.silver._
import org.apache.spark.sql.streaming.Trigger


// TODO: move to  the right place (TBD)
case class TableProperties(database: String, table: String, location: String)

// TODO: use one database, need to be re-evaluated after the finishing the modeling exercise
// TODO: class will be renamed: <project_nameConfiguration>
case class Configuration(
                          rootPath: String,
                          database: String,
                          checkpointDir: Path,
                          trigger: Trigger,
                          bronzeTable: String,
                          postInfoTable: String,
                        ) {
  def init(spark: SparkSession, overwrite: Boolean = false): Unit = {
    // TODO: check if init is done successfully
    initDatabase(spark, overwrite)
    initCheckpointDir(overwrite)
    initBronzeTable(spark, overwrite)
    initPostInfoTable(spark, overwrite)
  }

  def initDatabase(spark: SparkSession, overwrite: Boolean = false): Boolean = {
    val dbLocation = new Path(s"$rootPath/$database")
    val fs = getFileSystem(new Path(rootPath))
    fs.mkdirs(dbLocation)
    if (overwrite) {
      spark.sql(s"drop database if exists $database cascade")
      // TODO: use logger instead
      println(s"database $database successfully dropped")

      spark.sql(s"create database if not exists $database location '${dbLocation.toUri}'")
      // TODO: use logger instead
      println(s"database $database successfully created")
      true
    }
    else if (!spark.catalog.databaseExists(database)) {
      spark.sql(s"create database if not exists $database location '${dbLocation.toUri}'")
      // TODO: use logger instead
      println(s"database $database successfully created")
      true
    }
    else {
      // TODO: use logger instead
      println(s"database $database already exists")
      false
    }
  }

  def initBronzeTable(spark: SparkSession, overwrite: Boolean = false): Boolean = {
    import spark.implicits._
    val location = s"$rootPath/$database/$bronzeTable"
    val tableProperties = TableProperties(database, bronzeTable, location)
    val emptyConf: Seq[Data] = Seq()
    createTable(spark, emptyConf.toDF(), tableProperties, partitionColumns = null, overwrite)
  }

  def initPostInfoTable(spark: SparkSession, overwrite: Boolean = false): Boolean = {
    import spark.implicits._
    val location = s"$rootPath/$database/$postInfoTable"
    val tableProperties = TableProperties(database, postInfoTable, location)
    val emptyConf: Seq[SilverPostsInfo] = Seq()
    createTable(spark, emptyConf.toDF(), tableProperties, partitionColumns = null, overwrite)
  }

  def initCheckpointDir(overwrite: Boolean): Boolean = {
    mkdir(checkpointDir, overwrite)
  }
}

object Configuration {
  // TODO: names TBD
  val DATABASE = "instagram_db"
  val CHECKPOINT_DIR = "checkpoint_dir"
  val BRONZE_TABLE = "BronzeTable"
  val SILVER_POST_INFO_TABLE = "PostsInfoTable"

  def apply(basePath: String): Configuration = {
    val path = new Path(basePath)
    val fs = getFileSystem(path)
    if (!fs.exists(path)) {
      throw new FileNotFoundException(s"No such file or directory: $path")
    }
    val checkpointDir = new Path(s"${path.toString}/$CHECKPOINT_DIR")
    val trigger = Trigger.Once
    Configuration(
      basePath,
      DATABASE,
      checkpointDir,
      trigger,
      BRONZE_TABLE,
      SILVER_POST_INFO_TABLE
    )
  }

  // TODO: move the right place (TBD)
  def getFileSystem(path: Path): FileSystem = {
    val hadoopConfiguration = SparkSession.getActiveSession.map(x => x.sessionState.newHadoopConf()).get
    path.getFileSystem(hadoopConfiguration)
  }

  def getHadoopConfiguration: org.apache.hadoop.conf.Configuration = {
    SparkSession.getActiveSession.map(x => x.sessionState.newHadoopConf()).get
  }

  def makeQualified(path: Path): String = {
    path.getFileSystem(getHadoopConfiguration).makeQualified(path).toString
  }

  // TODO: user logger inside function
  def createTable(
                   spark: SparkSession,
                   df: DataFrame,
                   tableProperties: TableProperties,
                   partitionColumns: Option[Seq[String]] = null,
                   overwrite: Boolean = false): Boolean = {
    val database = tableProperties.database
    val table = tableProperties.table
    val location = tableProperties.location

    if (!spark.catalog.databaseExists(database)) {
      val dbLocation = new Path(location).getParent.toString
      spark.sql(s"create database if not exists $database location '${dbLocation}'")
    }
    if (!spark.catalog.tableExists(s"$database.$table") || overwrite) {
      persist(df, tableProperties)
      true
    }
    else {
      false
    }
  }

  def persist(df: DataFrame,
              tableProperties: TableProperties): Unit = {
      df
        .write
        .format("delta")
        .mode("overwrite")
        .option("path", s"${tableProperties.location}")
        .saveAsTable(s"${tableProperties.database}.${tableProperties.table}")

  }

  // TODO: use logger inside function
  def mkdir(path: Path, overwrite: Boolean = false): Boolean = {
    val fs = getFileSystem(path)
    if (overwrite) {
      fs.delete(path, true)
      fs.mkdirs(path)
    }
    else if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
    else {
      false
    }
  }
}