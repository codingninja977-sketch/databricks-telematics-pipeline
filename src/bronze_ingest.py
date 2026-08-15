import sys
from pyspark.sql import functions as F


# ============================================================
# Configuration
# ============================================================

CATALOG = "telematics"
SCHEMA = sys.argv[1]

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_gps_events"

CHECKPOINT_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/landing/"
    "_checkpoints/bronze_gps_events"
)


print("========================================")
print("BRONZE INGESTION STARTED")
print("========================================")
print(f"Catalog          : {CATALOG}")
print(f"Schema           : {SCHEMA}")
print(f"Landing path     : {LANDING_PATH}")
print(f"Bronze table     : {BRONZE_TABLE}")
print(f"Checkpoint path  : {CHECKPOINT_PATH}")
print("========================================")


# ============================================================
# Read JSON files using Auto Loader
# ============================================================

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"{LANDING_PATH}/_schema/bronze_gps_events"
        )
        .load(LANDING_PATH)
)


# ============================================================
# Add ingestion metadata
# ============================================================

bronze_df = (
    df
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# ============================================================
# Write to Bronze Delta table
# ============================================================

query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
)


query.awaitTermination()


print("========================================")
print("BRONZE INGESTION COMPLETED")
print("========================================")