import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# Configuration
# ============================================================

CATALOG = "telematics"
SCHEMA = sys.argv[1]

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_gps_events"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.truck_daily_summary"


print("========================================")
print("GOLD AGGREGATION STARTED")
print("========================================")
print(f"Silver table : {SILVER_TABLE}")
print(f"Gold table   : {GOLD_TABLE}")
print("========================================")


# ============================================================
# Read Silver
# ============================================================

silver_df = spark.table(SILVER_TABLE)


# ============================================================
# Window for previous GPS point
# ============================================================

truck_window = (
    Window
    .partitionBy("truck_id")
    .orderBy("event_ts")
)


# ============================================================
# Get previous GPS coordinates
# ============================================================

gps_df = (
    silver_df
    .withColumn(
        "previous_lat",
        F.lag("lat").over(truck_window)
    )
    .withColumn(
        "previous_lon",
        F.lag("lon").over(truck_window)
    )
)


# ============================================================
# Haversine calculation
# ============================================================

earth_radius_km = 6371.0

lat1 = F.radians(F.col("previous_lat"))
lat2 = F.radians(F.col("lat"))

delta_lat = (
    F.radians(F.col("lat"))
    - F.radians(F.col("previous_lat"))
)

delta_lon = (
    F.radians(F.col("lon"))
    - F.radians(F.col("previous_lon"))
)


a = (
    F.pow(F.sin(delta_lat / 2), 2)
    +
    F.cos(lat1)
    * F.cos(lat2)
    * F.pow(F.sin(delta_lon / 2), 2)
)


distance_km = (
    2
    * earth_radius_km
    * F.asin(F.sqrt(a))
)


gps_with_distance = (
    gps_df
    .withColumn(
        "distance_km",
        F.when(
            F.col("previous_lat").isNull()
            | F.col("previous_lon").isNull(),
            F.lit(0.0)
        )
        .otherwise(distance_km)
    )
)


# ============================================================
# Event date
# ============================================================

gps_with_distance = (
    gps_with_distance
    .withColumn(
        "event_date",
        F.to_date("event_ts")
    )
)


# ============================================================
# Daily truck aggregation
# ============================================================

gold_df = (
    gps_with_distance
    .groupBy(
        "truck_id",
        "event_date",
        "make",
        "model",
        "region",
        "home_depot",
        "driver"
    )
    .agg(
        F.count("*").alias("total_events"),

        F.round(
            F.avg("speed_kmh"),
            2
        ).alias("avg_speed_kmh"),

        F.round(
            F.max("speed_kmh"),
            2
        ).alias("max_speed_kmh"),

        F.round(
            F.min("speed_kmh"),
            2
        ).alias("min_speed_kmh"),

        F.round(
            F.sum("distance_km"),
            2
        ).alias("distance_km"),

        (
            (
                F.unix_timestamp(F.max("event_ts"))
                -
                F.unix_timestamp(F.min("event_ts"))
            ) / 3600.0
        ).alias("active_hours")
    )
)


# ============================================================
# Write Gold
# ============================================================

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(GOLD_TABLE)
)


# ============================================================
# Display summary
# ============================================================

record_count = spark.table(GOLD_TABLE).count()

print("----------------------------------------")
print(f"Gold records created: {record_count}")
print("----------------------------------------")

print("========================================")
print("GOLD AGGREGATION COMPLETED")
print("========================================")