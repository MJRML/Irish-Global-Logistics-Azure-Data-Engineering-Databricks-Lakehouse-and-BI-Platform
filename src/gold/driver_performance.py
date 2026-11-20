# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("global_logistics.catalog")

# COMMAND ----------

@dp.materialized_view(name="gold_driver_performance")
def gold_driver_performance():

    # Read Silver datasets
    drivers = spark.read.table(f"{catalog}.silver.drivers")
    deliveries = spark.read.table(f"{catalog}.silver.deliveries")
    working_hours = spark.read.table(f"{catalog}.silver.driver_working_hours")

    # Aggregate monthly delivery performance for each driver
    monthly_deliveries = (
        deliveries
        .withColumn("delivery_year", F.year("delivery_date"))
        .withColumn("delivery_month", F.month("delivery_date"))
        .groupBy("driver_id", "delivery_year", "delivery_month")
        .agg(
            F.count("delivery_id").alias("total_deliveries"),
            F.sum(
                F.when(F.col("delivery_status") == "Delivered", 1).otherwise(0)
            ).alias("successful_deliveries"),
            F.sum(
                F.when(F.col("delivery_status") == "Failed", 1).otherwise(0)
            ).alias("failed_deliveries")
        )
    )

    # Calculate the monthly delivery success rate
    monthly_deliveries = (
        monthly_deliveries
        .withColumn(
            "delivery_success_rate",
            F.round(
                F.col("successful_deliveries") /
                F.col("total_deliveries") * 100,
                2
            )
        )
    )

    # Calculate net working hours per shift, excluding breaks
    shift_hours = (
        working_hours
        .withColumn("shift_year", F.year("shift_start"))
        .withColumn("shift_month", F.month("shift_start"))
        .withColumn(
            "hours_worked",
            (
                F.unix_timestamp("shift_end")
                - F.unix_timestamp("shift_start")
                - (F.col("break_minutes") * 60)
            ) / 3600
        )
    )

    # Aggregate monthly working hours and completed shifts per driver
    monthly_working_hours = (
        shift_hours
        .groupBy("driver_id", "shift_year", "shift_month")
        .agg(
            F.round(F.sum("hours_worked"), 2).alias("total_hours_worked"),
            F.countDistinct("shift_id").alias("completed_shifts")
        )
        .withColumn(
            "average_hours_per_shift",
            F.round(
                F.col("total_hours_worked") /
                F.col("completed_shifts"),
                2
            )
        )
    )

    # Align the working-hours reporting columns with delivery reporting
    # The full outer join preserves months where a driver has shifts but no deliveries, or deliveries but no shifts.
    monthly_working_hours = (
        monthly_working_hours
        .withColumnRenamed("shift_year", "delivery_year")
        .withColumnRenamed("shift_month", "delivery_month")
    )

    # Combine delivery and working-hours metrics by driver and month
    monthly_driver_performance = (
        monthly_deliveries
        .join(
            monthly_working_hours,
            on=["driver_id", "delivery_year", "delivery_month"],
            how="full_outer"
        )
    )

    # Add driver details to the monthly performance metrics
    monthly_driver_performance = (
        monthly_driver_performance.alias("m")
        .join(
            drivers.alias("dr"),
            on="driver_id",
            how="left"
        )
        .select(
            "m.*",
            F.col("dr.depot_id"),
            F.concat_ws(
                " ",
                F.col("dr.first_name"),
                F.col("dr.last_name")
            ).alias("driver_name"),
            F.col("dr.employment_status").alias("driver_employment_status")
        )
    )

    # Create a readable reporting month for BI dashboards
    monthly_driver_performance = (
        monthly_driver_performance
        .withColumn(
            "delivery_month_year",
            F.date_format(
                F.make_date(
                    F.col("delivery_year"),
                    F.col("delivery_month"),
                    F.lit(1)
                ),
                "MMMM yyyy"
            )
        )
    )

    return monthly_driver_performance
