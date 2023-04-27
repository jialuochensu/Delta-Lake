// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("deltaLake_QuickStart").getOrCreate()

// COMMAND ----------

//to allow us to vacuum files shorter than the default retention duration of 7 days. Note, this is only required for the SQL command VACUUM
spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false")

// COMMAND ----------

//to enable Delta Lake SQL commands within Apache Spark; this is not required for Python or Scala API calls.
spark.sql("SET spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension")

// COMMAND ----------

//loading and savind Delta Lake Data
//location variables
val tripdelaysFilePath = "/FileStore/tables/departuredelays-1.csv"
val pathToEventsTable = "/FileStore/tables/departureDelays.delta"

// COMMAND ----------

//reader
val departureDelays = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(tripdelaysFilePath)

// COMMAND ----------

//writer
val writeDeparturedelays = departureDelays.write
  .format("delta")
  .mode("overwrite")
  .save("/FileStore/tables/departureDelays.delta")

// COMMAND ----------

//reload data from delta lake

val delays_delta = spark.read
  .format("delta")
  .load("/FileStore/tables/departureDelays.delta")

// COMMAND ----------

//create a temp view
delays_delta.createOrReplaceTempView("delays_delta")


//# How many flights are between Seattle and San Francisco
spark.sql("SELECT count(1) FROM delays_delta WHERE origin='SEA' and destination='SFO'").show()

// COMMAND ----------

//conversion to delta lake

