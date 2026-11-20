# Databricks notebook source
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    TimestampType
)

# COMMAND ----------

driver_working_hours_schema = StructType([
    StructField("shift_id", StringType(), False),
    StructField("driver_id", IntegerType(), True),
    StructField("depot_id", IntegerType(), True),
    StructField("shift_start", TimestampType(), True),
    StructField("shift_end", TimestampType(), True),
    StructField("break_minutes", IntegerType(), True),
    StructField("shift_status", StringType(), True)
])

# COMMAND ----------

depots_schema = StructType([
    StructField("depot_id", IntegerType(), False),
    StructField("depot_name", StringType(), False),
    StructField("city", StringType(), True),
    StructField("county", StringType(), True),
    StructField("created_date", TimestampType(), True)
])

# COMMAND ----------

vehicles_schema = StructType([
    StructField("vehicle_id", IntegerType(), False),
    StructField("registration_number", StringType(), False),
    StructField("vehicle_type", StringType(), True),
    StructField("depot_id", IntegerType(), False),
    StructField("vehicle_status", StringType(), True),
    StructField("created_date", TimestampType(), True)
])

# COMMAND ----------

drivers_schema = StructType([
    StructField("driver_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("licence_number", StringType(), False),
    StructField("depot_id", IntegerType(), False),
    StructField("employment_status", StringType(), False),
    StructField("created_date", TimestampType(), False)
])

# COMMAND ----------

deliveries_schema = StructType([
    StructField("delivery_id", IntegerType(), True),
    StructField("vehicle_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("depot_id", IntegerType(), True),
    StructField("delivery_date", TimestampType(), True),
    StructField("delivery_status", StringType(), True),
    StructField("delivery_city", StringType(), True),
    StructField("delivery_county", StringType(), True),
    StructField("modified_date", TimestampType(), True),
    StructField("failure_reason", StringType(), True)
])