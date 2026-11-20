# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../../05_utils/source_schemas

# COMMAND ----------

# MAGIC %run ../../05_utils/bronze_ingestion

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")

if catalog == "logistics_dev":
    schema_location = "abfss://databricks@stgloballogisticsdev.dfs.core.windows.net/schemas/employee_data/driver_working_hours/"
    checkpoint_location = "abfss://databricks@stgloballogisticsdev.dfs.core.windows.net/checkpoints/employee_data/driver_working_hours_new/"

elif catalog == "logistics_prod":
    schema_location = "abfss://databricks@stgloballogisticsdev.dfs.core.windows.net/schemas/prod/employee_data/driver_working_hours/"
    checkpoint_location = "abfss://databricks@stgloballogisticsdev.dfs.core.windows.net/checkpoints/prod/employee_data/driver_working_hours/"

else:
    raise ValueError(f"Unsupported catalog: {catalog}")

ingest_to_bronze(
    source_path="abfss://landing@stgloballogisticsdev.dfs.core.windows.net/driver_working_hours/",
    schema=driver_working_hours_schema,
    schema_location=schema_location,
    checkpoint_location=checkpoint_location,
    target_table=f"{catalog}.bronze.driver_working_hours"
)
