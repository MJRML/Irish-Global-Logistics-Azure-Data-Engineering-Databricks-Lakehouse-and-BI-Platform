# Databricks notebook source
from pyspark.sql import functions as F 

# COMMAND ----------

def ingest_to_bronze(
    source_path,
    schema,
    schema_location,
    checkpoint_location,
    target_table
):
    source_stream = (
        spark.readStream
        .format("cloudFiles")
        .schema(schema)
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("header", "true")
        .load(source_path)
    )

    bronze_df = (
        source_stream
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()