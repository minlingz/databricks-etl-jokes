# Databricks ETL Project

##  Project Overview
The purpose of this project is to use Auto Loader to ingest data (in this case, 4k+ jokes dataset stored in json files) from cloud object storage, process them using Spark and then loads the data into Delta table. By using the Delta table, we can query the data using SQL and visualize the data. With the visualization, we can see the distribution of the rating count and rating value of the jokes for different tags, and draw conclusions from the visualization.

## Project Files
* `README.md` - This file.
* `etl_databricks_jokes.py` - The main ETL script to process the data.

## Datasets
* `dbfs:/FileStore/shared_uploads/mz246@duke.edu/jokes/*.json` - The dataset contains the jokes data in cloud object storage.

## Data Pipeline
1. Ingest data from cloud object storage using Auto Loader.

```python
...
# Configure Auto Loader to ingest JSON data to a Delta table

    spark.readStream.format("cloudFiles")  # use Auto Loader
    .option("cloudFiles.format", "json")  # read JSON files
    .option(
        "cloudFiles.schemaLocation", checkpoint_path
    )  # use checkpoint location to store schema
    .load(file_location)  # read files from file_location
...
```
2. Process the data using Spark.
```python
...
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
...
```
3. Load the data into Delta table.
```python
...
    .writeStream.option(
        "checkpointLocation", checkpoint_path
    )  # checkpoint to avoid reprocessing files
    .trigger(availableNow=True)  # process files as soon as they appear
    .toTable(table_name)  # write to Delta table
...
```
4. Visualize the data.

![Alt text](ratingCountplot.png)

![Alt text](ratingValueplot.png)

## Conclusion
1. As displayed in the joke rating count plot, the distribution of the rating count of the jokes for different tags are not the same. `God`, `alcohol`, `blond`, `Christian`, `motocycle` topics have the highest rating count.
2. As displayed in the joke rating value plot, the distribution of the rating value of the jokes for different tags are not the same. `Father's day`, `Christian`, `friendship`, `motocycle`, `success` topics have the highest rating value.

## Future Work
We can explore more jokes sample dataset to create a topic/tag list that are more likely to have higher rating count and rating value. When generating jokes, we can generate jokes that are more popular and have higher rating value.