# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("global_logistics.catalog")

# COMMAND ----------

@dp.materialized_view(name="gold_depot_performance")
def gold_depot_performance():

    # 1. Read Silver tables
    
    deliveries = spark.read.table(
        f"{catalog}.silver.deliveries"
    )

    working_hours = spark.read.table(
        f"{catalog}.silver.driver_working_hours"
    )

    depots = spark.read.table(
        f"{catalog}.silver.depots"
    )

    # 2. Aggregate monthly delivery performance
    monthly_deliveries = (
        deliveries
        .withColumn("delivery_year", F.year("delivery_date"))
        .withColumn("delivery_month", F.month("delivery_date"))
        .groupBy("depot_id", "delivery_year", "delivery_month")
        .agg(
            F.count("delivery_id").alias("total_deliveries"),
            F.sum(F.when(F.col("delivery_status") == "Delivered", 1).otherwise(0)).alias("successful_deliveries"),
            F.sum(F.when(F.col("delivery_status") == "Failed", 1).otherwise(0)).alias("failed_deliveries"),
            F.sum(F.when(F.col("delivery_status") == "Delayed", 1).otherwise(0)).alias("delayed_deliveries"),
            F.sum(F.when(F.col("delivery_status") == "In Transit", 1).otherwise(0)).alias("in_transit_deliveries"),
            F.sum(F.when(F.col("delivery_status") == "Pending", 1).otherwise(0)).alias("pending_deliveries")
        )
    )

    # 3. Calculate working hours per shift
    shift_hours = (
        working_hours
        .withColumn("shift_year", F.year("shift_start"))
        .withColumn("shift_month", F.month("shift_start"))
        .withColumn(
            "hours_worked",
            (
                (F.unix_timestamp("shift_end") - F.unix_timestamp("shift_start")) / 60
                - F.col("break_minutes")
            ) / 60
        )
    )

    # 4. Aggregate working hours by depot and month
    monthly_working_hours = (
        shift_hours
        .groupBy("depot_id", "shift_year", "shift_month")
        .agg(
            F.round(F.sum("hours_worked"), 2).alias("total_employee_hours"),
            F.countDistinct("driver_id").alias("total_drivers_worked")
        )
        .withColumn(
            "average_hours_per_driver",
            F.round(
                F.col("total_employee_hours") / F.col("total_drivers_worked"),
                2
            )
        )
        .withColumnRenamed("shift_year", "delivery_year")
        .withColumnRenamed("shift_month", "delivery_month")
    )

    # 5. Join monthly deliveries and working hours
    monthly_depot_performance = (
        monthly_deliveries.alias("d")
        .join(
            monthly_working_hours.alias("wh"),
            on=["depot_id", "delivery_year", "delivery_month"],
            how="left"
        )
        .select(
            "d.*",
            F.coalesce(F.col("wh.total_employee_hours"), F.lit(0)).alias("total_driver_hours"),
            F.coalesce(F.col("wh.total_drivers_worked"), F.lit(0)).alias("total_drivers_worked"),
            F.coalesce(F.col("wh.average_hours_per_driver"), F.lit(0)).alias("average_hours_per_driver")
        )
    )

    # 6. Calculate delivery success and failure rates
    monthly_depot_performance = (
        monthly_depot_performance
        .withColumn(
            "delivery_success_rate",
            F.round(
                F.col("successful_deliveries") / F.col("total_deliveries") * 100,
                2
            )
        )
        .withColumn(
            "delivery_failure_rate",
            F.round(
                F.col("failed_deliveries") / F.col("total_deliveries") * 100,
                2
            )
        )
    )

    # 7. Add depot details
    monthly_depot_performance = (
        monthly_depot_performance.alias("m")
        .join(
            depots.alias("dpt"),
            on="depot_id",
            how="left"
        )
        .select(
            "m.*",
            F.col("dpt.depot_name").alias("depot_name"),
            F.col("dpt.city").alias("depot_city"),
            F.col("dpt.county").alias("depot_county")
        )
    )

    # 8. Add readable month-year label
    monthly_depot_performance = (
        monthly_depot_performance
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

    return monthly_depot_performance
