# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("global_logistics.catalog")

# COMMAND ----------


@dp.materialized_view(name="gold_delivery_performance")
def gold_delivery_performance():

    # 1. Read Silver business tables

    deliveries = spark.read.table(
        f"{catalog}.silver.deliveries"
    ).alias("d")

    depots = spark.read.table(
        f"{catalog}.silver.depots"
    ).alias("dpt")

    drivers = spark.read.table(
        f"{catalog}.silver.drivers"
    ).alias("dr")

    vehicles = spark.read.table(
        f"{catalog}.silver.vehicles"
    ).alias("v")

    # 2. Join the Silver datasets
    df = (
        deliveries
        .join(depots, on="depot_id", how="left")
        .join(drivers, on="driver_id", how="left")
        .join(vehicles, on="vehicle_id", how="left")
    )

    # 3. Select business-ready columns and derive metrics
    return df.select(

        # Delivery identification
        F.col("d.delivery_id"),
        F.col("d.delivery_date"),

        # Date dimensions
        F.year("d.delivery_date").alias("delivery_year"),
        F.month("d.delivery_date").alias("delivery_month"),
        F.dayofmonth("d.delivery_date").alias("delivery_day"),
        F.date_format(
            "d.delivery_date", "yyyy-MM"
        ).alias("delivery_month_year"),

        # Depot information
        F.col("d.depot_id"),
        F.col("dpt.depot_name"),
        F.col("dpt.city").alias("depot_city"),
        F.col("dpt.county").alias("depot_county"),

        # Driver information
        F.col("d.driver_id"),
        F.concat_ws(
            " ",
            F.col("dr.first_name"),
            F.col("dr.last_name")
        ).alias("driver_name"),
        F.col("dr.employment_status")
         .alias("driver_employment_status"),

        # Vehicle information
        F.col("d.vehicle_id"),
        F.col("v.registration_number"),
        F.col("v.vehicle_type"),
        F.col("v.vehicle_status"),

        # Delivery details
        F.col("d.delivery_city"),
        F.col("d.delivery_county"),
        F.col("d.delivery_status"),
        F.col("d.failure_reason"),

        # Delivery performance flags
        F.when(
            F.col("d.delivery_status") == "Delivered", 1
        ).otherwise(0).alias("delivery_success_flag"),

        F.when(
            F.col("d.delivery_status") == "Failed", 1
        ).otherwise(0).alias("delivery_failure_flag"),

        F.when(
            F.col("d.delivery_status") == "Delivered", 1
        ).otherwise(0).alias("delivery_completed_flag"),

        F.when(
            F.col("d.delivery_status") == "Delayed", 1
        ).otherwise(0).alias("delivery_delayed_flag"),

        F.when(
            F.col("d.delivery_status") == "In Transit", 1
        ).otherwise(0).alias("delivery_in_transit_flag"),

        F.when(
            F.col("d.delivery_status") == "Pending", 1
        ).otherwise(0).alias("delivery_pending_flag"),

        # Data lineage
        F.col("d._source_file"),
        F.col("d._ingestion_timestamp")
    )


# COMMAND ----------

