# Databricks notebook source
from pyspark.sql import functions as F
from pyspark import pipelines as dp

# COMMAND ----------

# Read the curated Silver datasets needed for driver performance reporting
drivers = spark.read.table(
    "logistics_dev.silver.drivers"
)

deliveries = spark.read.table(
    'logistics_dev.silver.deliveries'
)

working_hours = spark.read.table(
    "logistics_dev.silver.driver_working_hours"
)

# COMMAND ----------

# Aggregate delivery performance for each driver by reporting month
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
        "driver_id",
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
        ).alias("failed_deliveries")
    )
)

# COMMAND ----------

display(monthly_deliveries.limit(5))

# COMMAND ----------

#calculate the percentage of successful deliveries for each driver per month

monthly_deliveries = (
    monthly_deliveries
    .withColumn(
        "delivery_success_rate",
        F.round(
            F.col("successful_deliveries") / F.col("total_deliveries") * 100,
            2
        )
        
    )
)


# COMMAND ----------

display(monthly_deliveries.limit(5))

# COMMAND ----------

# Calculate net working hours for each shift, excluding unpaid breaks
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

# Aggregate working hours and completed shifts per driver and month
monthly_working_hours = (
    shift_hours
    .groupBy(
        "driver_id",
        "shift_year",
        "shift_month"
    )
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

# COMMAND ----------

display(monthly_working_hours.limit(5))

# COMMAND ----------

# Align the working-hours month columns with the delivery month columns

monthly_working_hours = (
    monthly_working_hours
    .withColumnRenamed("shift_year", "delivery_year")
    .withColumnRenamed("shift_month", "delivery_month")
)

#Combine delivery and performance and working working-hours-metrics per driver and month
monthly_driver_performance = (
    monthly_deliveries.alias("d")
    .join(
        monthly_working_hours,
        on = ["driver_id", "delivery_year", "delivery_month"],
        how = "full_outer"
    )
)

# COMMAND ----------

display(monthly_driver_performance.limit(5))

# COMMAND ----------

# Enrich monthly performance metrics with driver details
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
        F.concat_ws(" ", F.col("dr.first_name"), F.col("dr.last_name")).alias("driver_name"),
        F.col("dr.employment_status")
         .alias("driver_employment_status")
    )
)

# COMMAND ----------

display(monthly_driver_performance.limit(5))

# COMMAND ----------

#Create a readable reporting month for BI dashabords

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

# COMMAND ----------

display(monthly_driver_performance.limit(5))

# COMMAND ----------

