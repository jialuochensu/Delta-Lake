// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
val spark = SparkSession
  .builder()
  .appName("deltaLake")
  .getOrCreate()

// COMMAND ----------

//Create a range of numbers with a column named number
val myRange = spark.range(1000).toDF("number")
myRange.show()

// COMMAND ----------

val divBy2 = myRange.where("number % 2 = 0")
divBy2.show(5)

// COMMAND ----------

val filePath = "/FileStore/tables/2015_summary.csv"
val flightData2015 = spark.read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv(filePath)

// COMMAND ----------

flightData2015.take(3)

// COMMAND ----------

flightData2015.sort("count").explain()

// COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")

// COMMAND ----------

val sqlWay = spark.sql("""
  SELECT DEST_COUNTRY_NAME, count(1)
  FROM flight_data_2015
  GROUP BY DEST_COUNTRY_NAME
""")
val dfWay = flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .count()
sqlWay.explain()
dfWay.explain()

// COMMAND ----------


spark.sql("""
  SELECT max(count)
  FROM flight_data_2015
""").show()

flightData2015.select(max("count")).show()

// COMMAND ----------

//top 5 destination
val maxSql = spark.sql("""
  SELECT DEST_COUNTRY_NAME, sum(count) as Counter
  FROM flight_data_2015
  GROUP BY DEST_COUNTRY_NAME
  ORDER by Counter DESC
  LIMIT 5
""").show()

val maxDF = flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .agg(sum("count").alias("counter"))
  .orderBy(desc("counter"))
  .limit(5).show()

// COMMAND ----------

//Scala case class

case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

val flightsDF = spark.read
  .parquet("/FileStore/tables/2010-summary.parquet")
val flights = flightsDF.as[Flight]

// COMMAND ----------

flights
  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
  .take(5)

// COMMAND ----------

val staticDataFrame = spark
  .read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
val staticSchema = staticDataFrame.schema

// COMMAND ----------

display(staticDataFrame)

// COMMAND ----------

import org.apache.spark.sql.functions._

staticDataFrame
  .selectExpr(
  "CustomerID",
  "(UnitPrice * Quantity) as total_cost",
  "InvoiceDate"
   )
  .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")
  .show(5)

// COMMAND ----------

//read as streaming
val streamingDataFrame = spark
  .readStream
  .format("csv")
  .schema(staticSchema)
  .option("maxFilesPerTrigger", 1)
  .option("header", "true")
  .load("/FileStore/tables/retail-data/by-day/*.csv")

// COMMAND ----------

streamingDataFrame.isStreaming //check if its a stream

// COMMAND ----------

val purchaseByCustomerPerHour = streamingDataFrame
  .selectExpr(
  "CustomerID",
  "(UnitPrice * Quantity) as total_cost",
  "InvoiceDate"
   )
  .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")

// COMMAND ----------

//MLlib
staticDataFrame.printSchema()

// COMMAND ----------

val preppedDataFrame = staticDataFrame
  .na.fill(0)
  .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
  .coalesce(5)

// COMMAND ----------

//split for train and test set
val trainDataFrame = preppedDataFrame
  .where("InvoiceDate < '2011-07-01'")

val testDataFrame = preppedDataFrame
  .where("InvoiceDate >= '2011-07-01'")

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val indexer = new StringIndexer()
.setInputCol("day_of_week")
.setOutputCol("day_of_week_index")

// COMMAND ----------

import org.apache.spark.ml.feature.OneHotEncoder
val encoder = new OneHotEncoder()
.setInputCol("day_of_week_index")
.setOutputCol("day_of_week_encoded")

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
val vectorAssembler = new VectorAssembler()
 .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
 .setOutputCol("features")


// COMMAND ----------

import org.apache.spark.ml.Pipeline
val transformationPipeline = new Pipeline()
.setStages(Array(indexer, encoder, vectorAssembler))

// COMMAND ----------

val fittedPipeline = transformationPipeline.fit(trainDataFrame)

// COMMAND ----------

val transformedTraining = fittedPipeline.transform(trainDataFrame)

// COMMAND ----------

transformedTraining.cache()

// COMMAND ----------

import org.apache.spark.ml.clustering.KMeans
val kmeans = new KMeans()
 .setK(20)
 .setSeed(1L)

// COMMAND ----------

val kmModel = kmeans.fit(transformedTraining)

// COMMAND ----------

val transformedTest = fittedPipeline.transform(testDataFrame)

// COMMAND ----------

//Lower level APIs
spark.sparkContext.parallelize(Seq(1,2,3)).toDF()

// COMMAND ----------

import org.apache.spark.sql.functions._
df.select(lit(5), lit("five"), lit(5.0))

// COMMAND ----------

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/2010_12_01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

// COMMAND ----------

df.where(col("InvoiceNo").equalTo(536365))
  .select("InvoiceNo", "Description")
  .show(5, false)

// COMMAND ----------

val priceFilter = col("UnitPrice")>600
val descripFilter = col("Description").contains("POSTAGE")
df.where(col("StockCode").isin("DOT"))
  .where(priceFilter.or(descripFilter))
  .show()

// COMMAND ----------

df.select(
  round(col("UnitPrice"), 2).alias("rounder"),
  bround(col("UnitPrice"), 2).alias("brounder"),
  col("UnitPrice")
)
  .show(5)

// COMMAND ----------

df.select(
round(lit("2.5")),
bround(lit("2.5")))
.show(2)

// COMMAND ----------

df.describe().show()


// COMMAND ----------

df.select(monotonically_increasing_id()).show(2)

// COMMAND ----------

val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")

df.select(
  regexp_replace(col("Description"), regexString, "COLOR").alias("color_cleaned"),
  col("Description")
).show()

// COMMAND ----------

val dateDF = spark.range(10)
  .withColumn("today", current_date())
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")

// COMMAND ----------

dateDF.printSchema()
dateDF.show()

// COMMAND ----------

dateDF.select(
  date_sub(col("today"), 5),
  date_add(col("today"), 7)
  ).show(1)

// COMMAND ----------

dateDF
.withColumn("week_ago", date_sub(col("today"), 7))
.select(datediff(col("week_ago"), col("today")))
.show(1)
dateDF
.select(
to_date(lit("2016-01-01")).alias("start"),
to_date(lit("2017-05-22")).alias("end"))
.select(months_between(col("start"), col("end")))
.show(1)

// COMMAND ----------

val dateFormat = "yyyy-dd-MM"

val cleanDateDF = spark.range(1)
  .select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2")
  )
cleanDateDF.show()

// COMMAND ----------

cleanDateDF
 .select(
 to_timestamp(col("date"), dateFormat))
 .show()

// COMMAND ----------


