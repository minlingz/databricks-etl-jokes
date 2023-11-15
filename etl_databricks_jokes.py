# Databricks notebook source
import requests
from pyspark.sql.functions import col, current_timestamp, explode, split, regexp_extract
from pyspark.sql.types import *

# Define variables used in code below
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_jokes"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_jokes"
file_location = "dbfs:/FileStore/shared_uploads/mz246@duke.edu/jokes/"

# Clear out data
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_location)
  .withColumn("ratingCount", col("ratingCount").cast(IntegerType()))
  .withColumn("ratingValue", col("ratingValue").cast(DoubleType()))
  .select("*", explode(split(col("tags"), ",")).alias("tag_raw"))
  .withColumn("tag", regexp_extract(col("tag_raw"), '\\"(.*)\\"', 1))
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .drop("tag_raw")
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

df = spark.read.table(table_name)
display(df)
