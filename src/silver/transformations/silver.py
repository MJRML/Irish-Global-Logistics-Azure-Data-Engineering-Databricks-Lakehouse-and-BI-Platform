
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read Bronze from the catalog configured by the Silver pipeline in databricks.yml.
catalog = spark.conf.get("global_logistics.catalog")

@dp.materialized_view(
    name="depots_validated",
    comment="Depot records checked for data quality"
)

@dp.expect_all({
    "valid_depot_id": "depot_id IS NOT NULL",
    "valid_depot_name": "depot_name IS NOT NULL",
    "valid_city": "city IS NOT NULL",
    "valid_county": "county IS NOT NULL"
})

def depots_validated():

    return spark.read.table(f"{catalog}.bronze.depots")
  
@dp.materialized_view(
    name="depots_quarantine",
    comment="Depot records rejected during Silver validation"
)
def depots_quarantine():

    df = spark.read.table("depots_validated")

    window_spec = (
        Window
        .partitionBy("depot_id")
        .orderBy(
            F.col("_ingestion_timestamp").desc(),
            F.col("_source_file").desc()
        )
    )

    df = df.withColumn(
        "row_num",
        F.row_number().over(window_spec)
    )

    return (
        df
        .filter(
            F.col("depot_id").isNull() |
            F.col("depot_name").isNull() |
            F.col("city").isNull() |
            F.col("county").isNull() |
            (F.col("row_num") > 1)
        )
        
        .withColumn(
            "quarantine_reason",
            F.when(F.col("depot_id").isNull(), "MISSING_DEPOT_ID")
            .when(F.col("depot_name").isNull(), "MISSING_DEPOT_NAME")
            .when(F.col("city").isNull(), "MISSING_CITY")
            .when(F.col("county").isNull(), "MISSING_COUNTY")
            .otherwise("DUPLICATE_DEPOT_ID")
        )
        .drop("row_num")
    )


@dp.materialized_view(
    name="depots",
    comment="Validated and deduplicated depot records"
)

def silver_depots():

    df = spark.read.table("depots_validated")

    window_spec = (
        Window
        .partitionBy("depot_id")
        .orderBy(
            F.col("_ingestion_timestamp").desc(),
            F.col("_source_file").desc()
        )
    )

    return (
        df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(
            (F.col("row_num") == 1) &
            F.col("depot_id").isNotNull() &
            F.col("depot_name").isNotNull() &
            F.col("city").isNotNull() &
            F.col("county").isNotNull()
        )
        .drop("row_num")
        .withColumn(
            "created_date",
             F.to_date("created_date")
        )
    )

@dp.materialized_view(
    name="vehicles_validated",
    comment="Vehicle records checked for data quality"
)

@dp.expect_all({
    "valid_vehicle_id": "vehicle_id IS NOT NULL",
    "valid_registration_number": "registration_number IS NOT NULL",
    "valid_vehicle_type": "vehicle_type IS NOT NULL",
    "valid_depot_id": "depot_id IS NOT NULL",
    "valid_vehicle_status": "vehicle_status IS NOT NULL"
})

def vehicles_validated():

    return spark.read.table(f"{catalog}.bronze.vehicles")

@dp.materialized_view(
    name="vehicles_quarantine",
    comment="Vehicle records rejected during Silver validation"
)
def vehicles_quarantine():

    df = spark.read.table("vehicles_validated")

    # Check each vehicle references an existing depot
    depots = (
        spark.read.table("depots")
        .select("depot_id")
        .withColumn("_depot_exists", F.lit(True))
    )

    df = df.join(depots, on="depot_id", how="left")

    # Identify duplicate vehicles
    window_spec = (
        Window
        .partitionBy("vehicle_id")
        .orderBy(
            F.col("_ingestion_timestamp").desc(),
            F.col("_source_file").desc()
        )
    )

    df = df.withColumn(
        "row_num",
        F.row_number().over(window_spec)
    )

    return (
        df
        .filter(
            F.col("vehicle_id").isNull() |
            F.col("registration_number").isNull() |
            F.col("vehicle_type").isNull() |
            F.col("depot_id").isNull() |
            F.col("vehicle_status").isNull() |
            F.col("_depot_exists").isNull() |
            (F.col("row_num") > 1)
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("vehicle_id").isNull(), "MISSING_VEHICLE_ID")
            .when(F.col("registration_number").isNull(), "MISSING_REGISTRATION_NUMBER")
            .when(F.col("vehicle_type").isNull(), "MISSING_VEHICLE_TYPE")
            .when(F.col("depot_id").isNull(), "MISSING_DEPOT_ID")
            .when(F.col("vehicle_status").isNull(), "MISSING_VEHICLE_STATUS")
            .when(F.col("_depot_exists").isNull(), "INVALID_DEPOT_ID")
            .otherwise("DUPLICATE_VEHICLE_ID")
        )
        .drop("row_num", "_depot_exists")
    )

@dp.materialized_view(
    name="vehicles",
    comment="Validated and deduplicated vehicle records"
)
def silver_vehicles():

    df = spark.read.table("vehicles_validated")

    # Identify the latest record for each vehicle
    window_spec = (
        Window
        .partitionBy("vehicle_id")
        .orderBy(
            F.col("_ingestion_timestamp").desc(),
            F.col("_source_file").desc()
        )
    )

    # Keep only the latest valid vehicle record
    return (
        df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(
            (F.col("row_num") == 1) &
            F.col("vehicle_id").isNotNull() &
            F.col("registration_number").isNotNull() &
            F.col("vehicle_type").isNotNull() &
            F.col("depot_id").isNotNull() &
            F.col("vehicle_status").isNotNull()
        )
        .join(
            spark.read.table("depots").select("depot_id"),
            on="depot_id",
            how="inner"
        )
        .drop("row_num")
        .withColumn("created_date", F.to_date("created_date"))
    )

@dp.materialized_view(
    name="drivers_validated",
    comment="Driver records checked for data quality"
)

@dp.expect_all({
    "valid_driver_id": "driver_id IS NOT NULL",
    "valid_first_name": "first_name IS NOT NULL",
    "valid_last_name": "last_name IS NOT NULL",
    "valid_licence_number": "licence_number IS NOT NULL",
    "valid_depot_id": "depot_id IS NOT NULL",
    "valid_employment_status": "employment_status IS NOT NULL"
})

def drivers_validated():

    return spark.read.table(f"{catalog}.bronze.drivers")

@dp.materialized_view(
    name="drivers_quarantine",
    comment="Driver records rejected during Silver validation"
)
def drivers_quarantine():

    df = spark.read.table("drivers_validated")

    # Check each driver references an existing depot
    depots = (
        spark.read.table("depots")
        .select("depot_id")
        .withColumn("_depot_exists", F.lit(True))
    )

    df = df.join(depots, on="depot_id", how="left")

    # Identify the latest record for each driver
    driver_window = (
        Window
        .partitionBy("driver_id")
        .orderBy(
            F.col("_ingestion_timestamp").desc(),
            F.col("_source_file").desc()
        )
    )

    df = df.withColumn(
        "driver_row_num",
        F.row_number().over(driver_window)
    )

    # Check licence uniqueness across latest driver records only
    latest_drivers = (
        df
        .filter(F.col("driver_row_num") == 1)
        .withColumn(
            "licence_count",
            F.count("*").over(Window.partitionBy("licence_number"))
        )
    )

    # Identify every driver involved in a licence conflict
    licence_conflicts = (
        latest_drivers
        .filter(F.col("licence_count") > 1)
        .select("driver_id")
        .withColumn("_duplicate_licence", F.lit(True))
    )

    df = df.join(licence_conflicts, on="driver_id", how="left")
    

    return (
        df
        .filter(
            F.col("driver_id").isNull() |
            F.col("first_name").isNull() |
            F.col("last_name").isNull() |
            F.col("licence_number").isNull() |
            F.col("depot_id").isNull() |
            F.col("employment_status").isNull() |
            F.col("_depot_exists").isNull() |
            (F.col("driver_row_num") > 1) |
            F.col("_duplicate_licence").isNotNull()
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("driver_id").isNull(), "MISSING_DRIVER_ID")
            .when(F.col("first_name").isNull(), "MISSING_FIRST_NAME")
            .when(F.col("last_name").isNull(), "MISSING_LAST_NAME")
            .when(F.col("licence_number").isNull(), "MISSING_LICENCE_NUMBER")
            .when(F.col("depot_id").isNull(), "MISSING_DEPOT_ID")
            .when(F.col("employment_status").isNull(), "MISSING_EMPLOYMENT_STATUS")
            .when(F.col("_depot_exists").isNull(), "INVALID_DEPOT_ID")
            .when(F.col("driver_row_num") > 1, "DUPLICATE_DRIVER_ID")
            .when(F.col("_duplicate_licence").isNotNull(), "DUPLICATE_LICENCE_NUMBER")
        )
        .drop(
            "driver_row_num",
            "_duplicate_licence",
            "_depot_exists"
        )
    )

@dp.materialized_view(
    name="drivers",
    comment="Validated and deduplicated driver records"
)
def silver_drivers():

    df = spark.read.table("drivers_validated")

    # Identify the latest record for each driver
    driver_window = (
        Window
        .partitionBy("driver_id")
        .orderBy(
            F.col("_ingestion_timestamp").desc(),
            F.col("_source_file").desc()
        )
    )

    df = df.withColumn(
        "driver_row_num",
        F.row_number().over(driver_window)
    )

    # Keep only the latest record for each driver
    df = df.filter(F.col("driver_row_num") == 1)

    # Identify licence numbers shared by multiple drivers
    licence_window = Window.partitionBy("licence_number")

    df = df.withColumn(
        "licence_count",
        F.count("*").over(licence_window)
    )

    return (
        df
        .filter(
            (F.col("licence_count") == 1) &
            F.col("driver_id").isNotNull() &
            F.col("first_name").isNotNull() &
            F.col("last_name").isNotNull() &
            F.col("licence_number").isNotNull() &
            F.col("depot_id").isNotNull() &
            F.col("employment_status").isNotNull()
        )
        .join(
            spark.read.table("depots").select("depot_id"),
            on="depot_id",
            how="inner"
        )
        .drop("driver_row_num", "licence_count")
        .withColumn("created_date", F.to_date("created_date"))
    )

@dp.table(
    name="deliveries_validated",
    comment="Delivery records checked for data quality"
)

@dp.expect_all({
    "valid_delivery_id": "delivery_id IS NOT NULL",
    "valid_delivery_status": """
        delivery_status IN (
            'Delivered',
            'In Transit',
            'Pending',
            'Delayed',
            'Failed'
        )
    """,
    "valid_delivery_city": """
        delivery_city IS NOT NULL
        AND TRIM(delivery_city) <> ''
    """,
    "valid_delivery_county": """
        delivery_county IS NOT NULL
        AND TRIM(delivery_county) <> ''
    """
})

def deliveries_validated():

    return spark.readStream.table(
        f"{catalog}.bronze.deliveries"
    )


@dp.table(
    name="deliveries_quarantine",
    comment="Delivery records rejected during Silver validation"
)
def deliveries_quarantine():

    df = spark.readStream.table("deliveries_validated")

    # Prepare reference datasets
    vehicles = (
        spark.read.table("vehicles")
        .select("vehicle_id")
        .withColumn("_vehicle_exists", F.lit(True))
    )

    drivers = (
        spark.read.table("drivers")
        .select("driver_id")
        .withColumn("_driver_exists", F.lit(True))
    )

    depots = (
        spark.read.table("depots")
        .select("depot_id")
        .withColumn("_depot_exists", F.lit(True))
    )

    # Validate vehicle, driver and depot references
    df = df.join(vehicles, on="vehicle_id", how="left")
    df = df.join(drivers, on="driver_id", how="left")
    df = df.join(depots, on="depot_id", how="left")

    # Identify invalid records
    return (
        df
        .filter(
            F.col("delivery_id").isNull() |
            F.col("delivery_status").isNull() |
            (~F.col("delivery_status").isin(
                "Delivered",
                "In Transit",
                "Pending",
                "Delayed",
                "Failed"
            )) |
            F.col("delivery_city").isNull() |
            (F.trim(F.col("delivery_city")) == "") |
            F.col("delivery_county").isNull() |
            (F.trim(F.col("delivery_county")) == "") |
            F.col("_vehicle_exists").isNull() |
            F.col("_driver_exists").isNull() |
            F.col("_depot_exists").isNull()
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("delivery_id").isNull(), "MISSING_DELIVERY_ID")
            .when(F.col("delivery_status").isNull(), "MISSING_DELIVERY_STATUS")
            .when(
                ~F.col("delivery_status").isin(
                    "Delivered", "In Transit", "Pending", "Delayed", "Failed"
                ),
                "INVALID_DELIVERY_STATUS"
            )
            .when(
                F.col("delivery_city").isNull() |
                (F.trim(F.col("delivery_city")) == ""),
                "MISSING_DELIVERY_CITY"
            )
            .when(
                F.col("delivery_county").isNull() |
                (F.trim(F.col("delivery_county")) == ""),
                "MISSING_DELIVERY_COUNTY"
            )
            .when(F.col("vehicle_id").isNull(), "MISSING_VEHICLE_ID")
            .when(F.col("_vehicle_exists").isNull(), "INVALID_VEHICLE_ID")
            .when(F.col("driver_id").isNull(), "MISSING_DRIVER_ID")
            .when(F.col("_driver_exists").isNull(), "INVALID_DRIVER_ID")
            .when(F.col("depot_id").isNull(), "MISSING_DEPOT_ID")
            .when(F.col("_depot_exists").isNull(), "INVALID_DEPOT_ID")
)
        .drop(
            "_vehicle_exists",
            "_driver_exists",
            "_depot_exists"
        )
    )


@dp.table(
    name="deliveries_clean",
    comment="Valid delivery records ready for incremental processing"
)
def deliveries_clean():

    df = spark.readStream.table("deliveries_validated")

    # Keep records with valid delivery fields
    df = df.filter(
        F.col("delivery_id").isNotNull() &
        F.col("delivery_status").isin(
            "Delivered",
            "In Transit",
            "Pending",
            "Delayed",
            "Failed"
        ) &
        F.col("delivery_city").isNotNull() &
        (F.trim(F.col("delivery_city")) != "") &
        F.col("delivery_county").isNotNull() &
        (F.trim(F.col("delivery_county")) != "")
    )

    # Keep only deliveries with valid reference IDs
    df = df.join(
        spark.read.table("vehicles").select("vehicle_id"),
        on="vehicle_id",
        how="inner"
    )

    df = df.join(
        spark.read.table("drivers").select("driver_id"),
        on="driver_id",
        how="inner"
    )

    df = df.join(
        spark.read.table("depots").select("depot_id"),
        on="depot_id",
        how="inner"
    )

    return df

# Create the target streaming table
dp.create_streaming_table(
    name="deliveries",
    comment="Clean deliveries with the latest version retained"
)

# Apply incremental changes to the Silver table
dp.create_auto_cdc_flow(
    target="deliveries",
    source="deliveries_clean",
    keys=["delivery_id"],
    sequence_by=F.struct(
        F.col("modified_date"),
        F.col("_ingestion_timestamp")
    ),
    stored_as_scd_type=1
)


@dp.table(
    name="driver_working_hours_validated",
    comment="Driver working hours records checked for data quality"
)

@dp.expect_all({
    "valid_shift_id": "shift_id IS NOT NULL AND TRIM(shift_id) <> ''",
    "valid_driver_id": "driver_id IS NOT NULL",
    "valid_depot_id": "depot_id IS NOT NULL",
    "valid_shift_start": "shift_start IS NOT NULL",
    "valid_shift_end": "shift_end IS NOT NULL",
    "valid_break_minutes": "break_minutes IS NOT NULL AND break_minutes >= 0",
    "valid_shift_status": "shift_status IS NOT NULL AND TRIM(shift_status) <> ''"
})

def driver_working_hours_validated():

    return spark.readStream.table(
        f"{catalog}.bronze.driver_working_hours"
    )



@dp.table(
    name="driver_working_hours_quarantine",
    comment="Driver working hours records rejected during Silver validation"
)
def driver_working_hours_quarantine():

    df = spark.readStream.table("driver_working_hours_validated")

    # Prepare reference datasets
    drivers = (
        spark.read.table("drivers")
        .select("driver_id")
        .withColumn("_driver_exists", F.lit(True))
    )

    depots = (
        spark.read.table("depots")
        .select("depot_id")
        .withColumn("_depot_exists", F.lit(True))
    )

    # Check driver and depot references
    df = df.join(drivers, on="driver_id", how="left")
    df = df.join(depots, on="depot_id", how="left")

    # Identify invalid records
    return (
        df
        .filter(
            F.col("shift_id").isNull() |
            (F.trim(F.col("shift_id")) == "") |
            F.col("driver_id").isNull() |
            F.col("_driver_exists").isNull() |
            F.col("depot_id").isNull() |
            F.col("_depot_exists").isNull() |
            F.col("shift_start").isNull() |
            F.col("shift_end").isNull() |
            (F.col("shift_end") <= F.col("shift_start")) |
            F.col("break_minutes").isNull() |
            (F.col("break_minutes") < 0) |
            F.col("shift_status").isNull() |
            (F.trim(F.col("shift_status")) == "")
        )
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("shift_id").isNull() |
                (F.trim(F.col("shift_id")) == ""),
                "MISSING_SHIFT_ID"
            )
            .when(F.col("driver_id").isNull(), "MISSING_DRIVER_ID")
            .when(F.col("_driver_exists").isNull(), "INVALID_DRIVER_ID")
            .when(F.col("depot_id").isNull(), "MISSING_DEPOT_ID")
            .when(F.col("_depot_exists").isNull(), "INVALID_DEPOT_ID")
            .when(F.col("shift_start").isNull(), "MISSING_SHIFT_START")
            .when(F.col("shift_end").isNull(), "MISSING_SHIFT_END")
            .when(F.col("shift_end") <= F.col("shift_start"), "INVALID_SHIFT_TIME")
            .when(F.col("break_minutes").isNull(), "MISSING_BREAK_MINUTES")
            .when(F.col("break_minutes") < 0, "INVALID_BREAK_MINUTES")
            .when(
                F.col("shift_status").isNull() |
                (F.trim(F.col("shift_status")) == ""),
                "MISSING_SHIFT_STATUS"
            )
        )
        .drop("_driver_exists", "_depot_exists")
    )


@dp.table(
    name="driver_working_hours_clean",
    comment="Valid driver working hours ready for incremental processing"
)
def driver_working_hours_clean():

    df = spark.readStream.table("driver_working_hours_validated")

    # Keep records with valid shift information
    df = df.filter(
        F.col("shift_id").isNotNull() &
        (F.trim(F.col("shift_id")) != "") &
        F.col("driver_id").isNotNull() &
        F.col("depot_id").isNotNull() &
        F.col("shift_start").isNotNull() &
        F.col("shift_end").isNotNull() &
        (F.col("shift_end") > F.col("shift_start")) &
        F.col("break_minutes").isNotNull() &
        (F.col("break_minutes") >= 0) &
        F.col("shift_status").isNotNull() &
        (F.trim(F.col("shift_status")) != "")
    )

    # Keep only shifts with valid driver and depot references
    df = df.join(
        spark.read.table("drivers").select("driver_id"),
        on="driver_id",
        how="inner"
    )

    df = df.join(
        spark.read.table("depots").select("depot_id"),
        on="depot_id",
        how="inner"
    )

    return df

# Create the target streaming table
dp.create_streaming_table(
    name="driver_working_hours",
    comment="Clean driver working hours with the latest shift version retained"
)

# Apply incremental changes to the Silver table
dp.create_auto_cdc_flow(
    target="driver_working_hours",
    source="driver_working_hours_clean",
    keys=["shift_id"],
    sequence_by=F.struct(
        F.col("_ingestion_timestamp"),
        F.col("_source_file")
    ),
    stored_as_scd_type=1
)







