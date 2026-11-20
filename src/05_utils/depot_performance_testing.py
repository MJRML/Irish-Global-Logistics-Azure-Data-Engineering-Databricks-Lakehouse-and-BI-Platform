# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------


# 1. Read Silver deliveries
deliveries = spark.read.table(
    "logistics_dev.silver.deliveries"
)

# 2. Aggregate delivery performance by depot and month
monthly_deliveries = (
    deliveries
    .withColumn(
        "delivery_year",
        F.year("delivery_date")
    )
    .withColumn(
        "delivery_month",
        F.month("delivery_date")
    )
    .groupBy(
        "depot_id",
        "delivery_year",
        "delivery_month"
    )
    .agg(
        F.count("delivery_id").alias("total_deliveries"),

        F.sum(
            F.when(F.col("delivery_status") == "Delivered", 1)
             .otherwise(0)
        ).alias("successful_deliveries"),

        F.sum(
            F.when(F.col("delivery_status") == "Failed", 1)
             .otherwise(0)
        ).alias("failed_deliveries"),

        F.sum(
            F.when(F.col("delivery_status") == "Delayed", 1)
             .otherwise(0)
        ).alias("delayed_deliveries"),

        F.sum(
            F.when(F.col("delivery_status") == "In Transit", 1)
             .otherwise(0)
        ).alias("in_transit_deliveries"),

        F.sum(
            F.when(F.col("delivery_status") == "Pending", 1)
             .otherwise(0)
        ).alias("pending_deliveries")
    )
)
    


# COMMAND ----------


# 3. Read Silver driver working hours
working_hours = spark.read.table(
    "logistics_dev.silver.driver_working_hours"
)

# 4. Calculate employee working hours per shift
shift_hours = (
    working_hours
    .withColumn(
        "shift_year",
        F.year("shift_start")
    )
    .withColumn(
        "shift_month",
        F.month("shift_start")
    )
    .withColumn(
        "hours_worked",
        (
            (F.unix_timestamp("shift_end") -
             F.unix_timestamp("shift_start")) / 60
            - F.col("break_minutes")
        ) / 60
    )
)

# 5. Aggregate working hours by depot and month
monthly_working_hours = (
    shift_hours
    .groupBy(
        "depot_id",
        "shift_year",
        "shift_month"
    )
    .agg(
        F.round(
            F.sum("hours_worked"), 2
        ).alias("total_employee_hours"),

        F.countDistinct("driver_id")
         .alias("total_drivers_worked")
    )
    .withColumn(
        "average_hours_per_driver",
        F.round(
            F.col("total_employee_hours") /
            F.col("total_drivers_worked"), 2
        )
    )
)


# COMMAND ----------

# Rename shift year/month to match delivery year/month
monthly_working_hours = (
    monthly_working_hours
    .withColumnRenamed("shift_year", "delivery_year")
    .withColumnRenamed("shift_month", "delivery_month")
)

# Join monthly delivery and working-hours aggregates
monthly_depot_performance = (
    monthly_deliveries.alias("d")
    .join(
        monthly_working_hours.alias("wh"),
        on=["depot_id", "delivery_year", "delivery_month"],
        how="left"
    )
    .select(
        "d.*",
        F.coalesce(
            F.col("wh.total_employee_hours"), F.lit(0)
        ).alias("total_driver_hours"),
        F.coalesce(
            F.col("wh.total_drivers_worked"), F.lit(0)
        ).alias("total_drivers_worked"),
        F.coalesce(
            F.col("wh.average_hours_per_driver"), F.lit(0)
        ).alias("average_hours_per_driver")
    )
)

# COMMAND ----------

monthly_depot_performance = (
    monthly_depot_performance
    .withColumn(
        "delivery_success_rate",
        F.round(
            F.col("successful_deliveries") /
            F.col("total_deliveries") * 100,
            2
        )
    )
    .withColumn(
        "delivery_failure_rate",
        F.round(
            F.col("failed_deliveries") /
            F.col("total_deliveries") * 100,
            2
        )
    )
)

# COMMAND ----------

