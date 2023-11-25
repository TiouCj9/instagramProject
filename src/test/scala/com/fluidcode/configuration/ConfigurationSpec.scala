package com.fluidcode.configuration

import java.nio.file.Paths

import com.fluidcode.configuration.Configuration._
import com.fluidcode.models.bronze.Data
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession

class ConfigurationSpec extends QueryTest
with SharedSparkSession
with DeltaExtendedSparkSession {

  // TODO: use temp dir instead for testing
  val basePath: String = Paths.get(getClass.getResource("/").toURI).getParent + "/instagram"

  def getTableProperties(database: String, table: String): TableProperties = {
  spark
  .sql(s"describe detail $database.$table")
  .select("name", "location")
  .collect().map {
  row =>
  val nameSplit = row.getString(0).split('.')
  TableProperties(nameSplit(0), nameSplit(1), row.getString(1))
}.head
}

  override def afterEach(): Unit = {
  super.afterEach()
  spark.catalog
  .listDatabases()
  .filter(_.name != "default")
  .collect()
  .map(db => spark.sql(s"drop database if exists ${db.name} cascade"))
}

  test("create database when initially exists") {
  val sparkSession = spark
  import sparkSession.implicits._

  val dbLocation = new Path(s"$basePath/$DATABASE")
  val fs = getFileSystem(dbLocation)
  val emptyUserManagedConf: Seq[Data] = Seq()
  val table = "my_table"
  val location = s"$dbLocation/$table/"
  val tableProperties = TableProperties(DATABASE, table, location)
  spark.sql(s"create database if not exists $DATABASE location '${dbLocation.toUri}'")
  createTable(spark, emptyUserManagedConf.toDF(), tableProperties)

  // database initially exists and is not empty
  assert(spark.catalog.databaseExists(DATABASE))
  assert(spark.catalog.listTables(DATABASE).count() != 0)

  // database location exists and is not empty
  assert(fs.exists(dbLocation))
  assert(fs.listStatus(dbLocation).length != 0)

  // init database
  val instagramConf = Configuration(basePath)
  instagramConf.initDatabase(spark, true)

  // an empty database is created
  assert(spark.catalog.databaseExists(DATABASE))
  assert(spark.catalog.listTables(DATABASE).count() == 0)

  // an empty database location is created
  assert(fs.exists(dbLocation))
  assert(fs.listStatus(dbLocation).length == 0)

  // clean
  spark.sql(s"drop database if exists $DATABASE cascade")
}

  // TODO: to be completed
  test("create database when initially exists and overwrite is false") {

}

  test("create database when initially not exists") {

  val dbLocation = new Path(s"$basePath/$DATABASE")
  val fs = getFileSystem(dbLocation)

  // database initially doesn't exists
  assert(!spark.catalog.databaseExists(DATABASE))

  // database location doesn't exists
  assert(!fs.exists(dbLocation))

  // init database
  val instagramConf = Configuration(basePath)
  instagramConf.initDatabase(spark)

  // an empty database is created
  assert(spark.catalog.databaseExists(DATABASE))
  assert(spark.catalog.listTables(DATABASE).count() == 0)

  // an empty database location is created
  assert(fs.exists(dbLocation))
  assert(fs.listStatus(dbLocation).length == 0)

  // clean
  spark.sql(s"drop database if exists $DATABASE cascade")
}

  // TODO: to be completed
  test("create database when initially not exists and overwrite is true") {

}

  test("create directory when not present") {
  withTempDir { basePath =>
  val dirName = "my_dir"
  val dirPath = new Path(s"$basePath/$dirName")
  val fs = getFileSystem(dirPath)

  // dir initially not present
  assert(!fs.exists(dirPath))

  // create dir
  mkdir(dirPath)

  // an empty dir is created
  assert(fs.exists(dirPath))
  assert(fs.listStatus(dirPath).length == 0)

}
}

  test("create directory when not present and overwrite is true") {
  withTempDir { basePath =>
  val dirName = "my_dir"
  val dirPath = new Path(s"$basePath/$dirName")
  val fs = getFileSystem(dirPath)

  // dir initially not present
  assert(!fs.exists(dirPath))

  // create dir
  mkdir(dirPath, true)

  // an empty dir is created
  assert(fs.exists(dirPath))
  assert(fs.listStatus(dirPath).length == 0)
}
}

  test("create directory when present and overwrite is true") {
  withTempDir { basePath =>
  val dirName = "my_dir"
  val childPath = new Path(s"$basePath/$dirName/child")
  val parentDir = childPath.getParent
  val fs = getFileSystem(childPath)

  // dir initially present
  mkdir(parentDir)
  mkdir(childPath)
  assert(fs.exists(parentDir))
  assert(fs.exists(childPath))
  assert(fs.listStatus(parentDir).length != 0)

  // create dir
  mkdir(parentDir, true)

  // an empty dir is created
  assert(fs.exists(parentDir))
  assert(!fs.exists(childPath))
  assert(fs.listStatus(parentDir).length == 0)
}
}

  test("create directory when present and overwrite is false") {
  withTempDir { basePath =>
  val dirName = "my_dir"
  val childPath = new Path(s"$basePath/$dirName/child")
  val parentDir = childPath.getParent
  val fs = getFileSystem(childPath)

  // dir initially present
  mkdir(parentDir)
  mkdir(childPath)
  assert(fs.exists(parentDir))
  assert(fs.exists(childPath))
  assert(fs.listStatus(parentDir).length != 0)

  // create dir
  mkdir(parentDir)

  // do nothing
  assert(fs.exists(parentDir))
  assert(fs.exists(childPath))
  assert(fs.listStatus(parentDir).length != 0)
}
}

  test("create table when not present and overwrite is false") {
  withTempDir { dir =>
  val sparkSession = spark
  import sparkSession.implicits._
  // Given
  val emptyDF: Seq[Data] = Seq()
  val database = "my_db"
  val table = "my_table"
  val location = s"${dir.toString}/$database/$table/"
  val tableProperties = TableProperties(database, table, location)

  // When
  createTable(spark, emptyDF.toDF(), tableProperties)

  // Then
  val createdTableProperties = getTableProperties(database, table)
  val expectedTableProperties = TableProperties(database, table, makeQualified(new Path(location)))
  assert(expectedTableProperties == createdTableProperties)
}
}

  test("create table table when not present and overwrite is true") {
  withTempDir { dir =>
  val sparkSession = spark
  import sparkSession.implicits._
  // Given
  val emptyDF: Seq[Data] = Seq()
  val database = "my_db"
  val table = "my_table"
  val location = s"${dir.toString}/$database/$table/"
  val tableProperties = TableProperties(database, table, location)

  // When
  createTable(spark, emptyDF.toDF(), tableProperties, overwrite = true)

  // Then
  val createdTableProperties = getTableProperties(database, table)
  val expectedTableProperties = TableProperties(database, table, makeQualified(new Path(location)))
  assert(expectedTableProperties == createdTableProperties)
}
}

  test("create table in a non existing database") {
  val sparkSession = spark
  import sparkSession.implicits._
  // Given
  val emptyDF: Seq[Data] = Seq()
  val database = "my_db"
  val table = "my_table"
  val location = s"$basePath/$database/$table/"
  val tableProperties = TableProperties(database, table, location)

  // When
  createTable(spark, emptyDF.toDF(), tableProperties)

  // Then
  val createdTableProperties = getTableProperties(database, table)
  val expectedTableProperties = TableProperties(database, table, makeQualified(new Path(location)))
  assert(expectedTableProperties == createdTableProperties)
}

  test("create table in an existing database") {
  val sparkSession = spark
  import sparkSession.implicits._
  // Given
  val emptyDF: Seq[Data] = Seq()
  val database = "my_db"
  val table = "my_table"
  val location = s"$basePath/$database/$table/"
  val tableProperties = TableProperties(database, table, location)

  // When
  sparkSession.sql(s"create database if not exists $database")
  createTable(spark, emptyDF.toDF(), tableProperties)

  // Then
  val createdTableProperties = getTableProperties(database, table)
  val expectedTableProperties = TableProperties(database, table, makeQualified(new Path(location)))
  assert(expectedTableProperties == createdTableProperties)
}

  test("init instagram conf") {
  withTempDir { dir =>
  val basePath = new Path(dir.toString)
  val fs = getFileSystem(basePath)

  val instagramConf = Configuration(dir.toString)
  instagramConf.init(spark)

  assert(fs.exists(new Path(s"${basePath.toString}/$CHECKPOINT_DIR")))
  assert(fs.exists(new Path(s"${basePath.toString}/$DATABASE")))
    assert(fs.exists(new Path(s"${basePath.toString}/$DATABASE/$BRONZE_TABLE")))
    assert(fs.exists(new Path(s"${basePath.toString}/$DATABASE/$SILVER_PROFILE_INFO_TABLE")))

    assert(spark.catalog.databaseExists(DATABASE))
    assert(spark.catalog.tableExists(s"$DATABASE.$BRONZE_TABLE"))
    assert(spark.catalog.tableExists(s"$DATABASE.$SILVER_PROFILE_INFO_TABLE"))
  }
}

  test("persist should persist a given dataframe in the given Table") {
  withTempDir { dir =>
  val sparkSession = spark
  import sparkSession.implicits._

  sql("create database my_db")
  sql("use my_db")
  val rootPath = dir.toString
  val database = "my_db"
  val table = "my_table"
  val location = s"$rootPath/$database/$table"
  val df = Seq(1 ,100, 1000, 10000).toDF("value")
  .withColumn("partitionColumn", col("value") * 2)
  .withColumn("otherPartitionColumn", col("value") * 4)
  val tableProperties = TableProperties(database, table, location)
  val partitionByColumns = Some(Seq("partitionColumn", "otherPartitionColumn"))

  persist(df, tableProperties)

  val partitionColumns = spark.sql("describe detail my_table").select("partitionColumns")
  val expectedPartitionColumns = Seq(Row(Seq("partitionColumn", "otherPartitionColumn")))
  val persistedDataFrame = spark.read.format("delta").table(s"$database.$table")

  assert(persistedDataFrame.except(df).isEmpty)

  sql("drop database my_db cascade")
}
}
}
