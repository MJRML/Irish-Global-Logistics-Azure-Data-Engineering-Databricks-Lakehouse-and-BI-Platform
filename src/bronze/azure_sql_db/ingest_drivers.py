# Databricks notebook source
# MAGIC %run ../../05_utils/source_schemas

# COMMAND ----------

# MAGIC %run ../../05_utils/bronze_ingestion

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")

if catalog == "logistics_dev":
    schema_location = "abfss://databricks@stgloballogisticsdev.dfs.core.windows.net/schemas/azure_sql/drivers/"
    checkpoint_location = "abfss://databricks@stgloballogisticsdev.dfs.core.windows.net/checkpoints/azure_sql/drivers/"

elif catalog == "logistics_prod":
    schema_location = "abfss://databricks@stgloballogisticsdev.dfs.core.windows.net/schemas/prod/azure_sql/drivers/"
    checkpoint_location = "abfss://databricks@stgloballogisticsdev.dfs.core.windows.net/checkpoints/prod/azure_sql/drivers/"

else:
    raise ValueError(f"Unsupported catalog: {catalog}")

ingest_to_bronze(
    source_path="abfss://landing@stgloballogisticsdev.dfs.core.windows.net/azure_sql/drivers/",
    schema=drivers_schema,
    schema_location=schema_location,
    checkpoint_location=checkpoint_location,
    target_table=f"{catalog}.bronze.drivers"
)
