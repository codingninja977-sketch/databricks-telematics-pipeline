import sys

CATALOG = "telematics"
SCHEMA = sys.argv[1]

from pyspark.sql import functions as F

trucks = [
    ("TRK-001", "Freightliner", "Cascadia", 20000, "Chicago", "Midwest", "A. Rivera"),
    ("TRK-002", "Volvo", "VNL", 26000, "Dallas", "South", "B. Chen"),
    ("TRK-003", "Kenworth", "T680", 34000, "Denver", "West", "C. Okafor"),
    ("TRK-004", "Peterbilt", "579", 40000, "Atlanta", "Southeast", "D. Patel"),
    ("TRK-005", "Freightliner", "Cascadia", 26000, "Chicago", "Midwest", "E. Nguyen"),
    ("TRK-006", "Volvo", "VNL", 34000, "Dallas", "South", "F. Santos"),
    ("TRK-007", "Kenworth", "T680", 40000, "Denver", "West", "G. Kim"),
    ("TRK-008", "Peterbilt", "579", 20000, "Atlanta", "Southeast", "H. Brooks"),
    ("TRK-009", "Freightliner", "Cascadia", 34000, "Chicago", "Midwest", "I. Novak"),
    ("TRK-010", "Volvo", "VNL", 40000, "Dallas", "South", "J. Alvarez"),
    ("TRK-011", "Kenworth", "T680", 26000, "Denver", "West", "A. Rivera"),
    ("TRK-012", "Peterbilt", "579", 34000, "Atlanta", "Southeast", "B. Chen"),
    ("TRK-013", "Freightliner", "Cascadia", 40000, "Chicago", "Midwest", "C. Okafor"),
    ("TRK-014", "Volvo", "VNL", 20000, "Dallas", "South", "D. Patel"),
    ("TRK-015", "Kenworth", "T680", 26000, "Denver", "West", "E. Nguyen"),
    ("TRK-016", "Peterbilt", "579", 34000, "Atlanta", "Southeast", "F. Santos"),
    ("TRK-017", "Freightliner", "Cascadia", 20000, "Chicago", "Midwest", "G. Kim"),
    ("TRK-018", "Volvo", "VNL", 26000, "Dallas", "South", "H. Brooks"),
    ("TRK-019", "Kenworth", "T680", 40000, "Denver", "West", "I. Novak"),
    ("TRK-020", "Peterbilt", "579", 34000, "Atlanta", "Southeast", "J. Alvarez"),
]

columns = [
    "truck_id",
    "make",
    "model",
    "capacity_lbs",
    "home_depot",
    "region",
    "driver",
]

df = spark.createDataFrame(trucks, columns)

target = f"{CATALOG}.{SCHEMA}.truck_details"

df.createOrReplaceTempView("truck_seed")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target} (
    truck_id STRING NOT NULL,
    make STRING,
    model STRING,
    capacity_lbs BIGINT,
    home_depot STRING,
    region STRING,
    driver STRING
)
""")

spark.sql(f"""
MERGE INTO {target} AS target
USING truck_seed AS source
ON target.truck_id = source.truck_id

WHEN NOT MATCHED THEN
  INSERT (
    truck_id,
    make,
    model,
    capacity_lbs,
    home_depot,
    region,
    driver
  )
  VALUES (
    source.truck_id,
    source.make,
    source.model,
    source.capacity_lbs,
    source.home_depot,
    source.region,
    source.driver
  )
""")