import sys

from pyspark.sql import functions as F


# ============================================================
# Configuration
# ============================================================

CATALOG = "telematics"
SCHEMA = sys.argv[1]

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_gps_events"
TRUCK_TABLE = f"{CATALOG}.{SCHEMA}.truck_details"

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_gps_events"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.quarantine_gps_events"
METRICS_TABLE = f"{CATALOG}.{SCHEMA}.gps_quality_metrics"

CHECKPOINT_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/landing/"
    "_checkpoints/silver_gps_events_v2"
)


print("========================================")
print("SILVER INGESTION STARTED")
print("========================================")
print(f"Bronze table     : {BRONZE_TABLE}")
print(f"Truck table      : {TRUCK_TABLE}")
print(f"Silver table     : {SILVER_TABLE}")
print(f"Quarantine table : {QUARANTINE_TABLE}")
print(f"Metrics table    : {METRICS_TABLE}")
print(f"Checkpoint       : {CHECKPOINT_PATH}")
print("========================================")


# ============================================================
# Read Bronze as a stream
# ============================================================

bronze_stream = (
    spark.readStream
        .table(BRONZE_TABLE)
)


# ============================================================
# Process each micro-batch
# ============================================================

def process_batch(batch_df, batch_id):

    print("----------------------------------------")
    print(f"Processing Silver batch: {batch_id}")
    print("----------------------------------------")

    if batch_df.isEmpty():
        print("Empty batch")
        return

    # ========================================================
    # Basic counts
    # ========================================================

    processed_count = batch_df.count()

    print(f"Records received: {processed_count}")


    # ========================================================
    # Convert event timestamp
    # ========================================================

    prepared_df = (
        batch_df
        .withColumn(
            "event_ts",
            F.to_timestamp("event_ts")
        )
    )


    # ========================================================
    # Validate GPS coordinates
    # ========================================================

    invalid_condition = (
        F.col("lat").isNull()
        | F.col("lon").isNull()
        | (F.col("lat") < -90)
        | (F.col("lat") > 90)
        | (F.col("lon") < -180)
        | (F.col("lon") > 180)
    )

    invalid_df = (
        prepared_df
        .filter(invalid_condition)
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("lat").isNull()
                | (F.col("lat") < -90)
                | (F.col("lat") > 90),
                F.lit("INVALID_LATITUDE")
            )
            .when(
                F.col("lon").isNull()
                | (F.col("lon") < -180)
                | (F.col("lon") > 180),
                F.lit("INVALID_LONGITUDE")
            )
            .otherwise(
                F.lit("INVALID_COORDINATES")
            )
        )
        .withColumn(
            "_quarantine_ts",
            F.current_timestamp()
        )
    )

    invalid_count = invalid_df.count()

    print(f"Invalid GPS records: {invalid_count}")


    # ========================================================
    # Write invalid records to quarantine
    # ========================================================

    if invalid_count > 0:

        (
            invalid_df
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(QUARANTINE_TABLE)
        )


    # ========================================================
    # Keep only valid GPS records
    # ========================================================

    valid_df = prepared_df.filter(~invalid_condition)


    # ========================================================
    # Detect duplicates inside the current batch
    # ========================================================

    duplicate_ids = (
        valid_df
        .groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .select("event_id")
    )


    duplicate_records = (
        valid_df
        .join(
            duplicate_ids,
            on="event_id",
            how="inner"
        )
    )


    duplicate_count = duplicate_records.count()

    print(f"Duplicate records: {duplicate_count}")


    # ========================================================
    # Keep one record per event_id
    # ========================================================

    deduplicated_df = (
        valid_df
        .dropDuplicates(["event_id"])
    )


    # ========================================================
    # Remove records already present in Silver
    # ========================================================

    if spark.catalog.tableExists(SILVER_TABLE):

        existing_ids = (
            spark.table(SILVER_TABLE)
            .select("event_id")
            .dropDuplicates()
        )

        new_records = (
            deduplicated_df
            .join(
                existing_ids,
                on="event_id",
                how="left_anti"
            )
        )

    else:

        new_records = deduplicated_df


    # ========================================================
    # Join truck details
    # ========================================================

    truck_df = spark.table(TRUCK_TABLE)

    enriched_df = (
        new_records
        .join(
            truck_df,
            on="truck_id",
            how="left"
        )
    )


    # ========================================================
    # Write Silver table
    # ========================================================

    silver_inserted_count = enriched_df.count()

    print(
        f"Records written to Silver: "
        f"{silver_inserted_count}"
    )

    if silver_inserted_count > 0:

        (
            enriched_df
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(SILVER_TABLE)
        )


    # ========================================================
    # Create metrics record
    # ========================================================

    metrics_df = spark.createDataFrame(
        [
            (
                int(batch_id),
                int(processed_count),
                int(invalid_count),
                int(duplicate_count),
                int(silver_inserted_count)
            )
        ],
        [
            "batch_id",
            "processed_count",
            "invalid_coordinate_count",
            "duplicate_count",
            "silver_inserted_count"
        ]
    ).withColumn(
        "processed_ts",
        F.current_timestamp()
    )


    # ========================================================
    # Write metrics
    # ========================================================

    (
        metrics_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(METRICS_TABLE)
    )


    print("----------------------------------------")
    print(f"Batch {batch_id} completed")
    print("----------------------------------------")


# ============================================================
# Start streaming query
# ============================================================

silver_query = (
    bronze_stream.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH
        )
        .trigger(availableNow=True)
        .start()
)


silver_query.awaitTermination()


print("========================================")
print("SILVER INGESTION COMPLETED")
print("========================================")