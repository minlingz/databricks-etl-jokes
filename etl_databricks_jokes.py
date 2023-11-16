# Databricks notebook source
import requests
from pyspark.sql.functions import col, current_timestamp, explode, split, regexp_extract
from pyspark.sql.types import *

# Define variables used in code below
username = spark.sql(
    "SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')"
).first()[0]
table_name = f"{username}_etl_jokes"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_jokes"
file_location = "dbfs:/FileStore/shared_uploads/mz246@duke.edu/jokes/"

# Clear out data
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(
    spark.readStream.format("cloudFiles")  # use Auto Loader
    .option("cloudFiles.format", "json")  # read JSON files
    .option(
        "cloudFiles.schemaLocation", checkpoint_path
    )  # use checkpoint location to store schema
    .load(file_location)  # read files from file_location
    .withColumn(
        "ratingCount", col("ratingCount").cast(IntegerType())
    )  # cast ratingCount to integer
    .withColumn(
        "ratingValue", col("ratingValue").cast(DoubleType())
    )  # cast ratingValue to double
    .select(
        "*", explode(split(col("tags"), ",")).alias("tag_raw")
    )  # explode tags into separate rows for each tag
    .withColumn(
        "tag", regexp_extract(col("tag_raw"), '\\"(.*)\\"', 1)
    )  # extract tag from tag_raw
    .select(
        "*",
        col("_metadata.file_path").alias(
            "source_file"
        ),  # add source file name from metadata
        current_timestamp().alias("processing_time"),  # add processing time
    )
    .drop("tag_raw")  # drop tag_raw column which is no longer needed
    .writeStream.option(
        "checkpointLocation", checkpoint_path
    )  # checkpoint to avoid reprocessing files
    .trigger(availableNow=True)  # process files as soon as they appear
    .toTable(table_name)  # write to Delta table
)


# COMMAND ----------

df = spark.read.table(table_name)  # read from Delta table
display(df)  # display data

# Data validation checks
assert (
    df.filter(df.ratingCount.isNull()).count() == 0
), "Null values found in ratingCount"
assert (
    df.filter(df.ratingValue.isNull()).count() == 0
), "Null values found in ratingValue"
assert (
    df.filter(df.ratingValue < 0).count() == 0
), "Negative values found in ratingValue"
assert (
    df.filter(df.ratingCount < 0).count() == 0
), "Negative values found in ratingCount"
