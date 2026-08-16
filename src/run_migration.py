import sys

CATALOG = "telematics"
SCHEMA = sys.argv[1]

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.truck_details"
COLUMN_NAME = "vehicle_type"

print("=" * 50)
print("RUNNING MIGRATION")
print("=" * 50)
print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Table   : {TARGET_TABLE}")
print("Migration: 002_add_vehicle_type")
print("=" * 50)

columns = spark.catalog.listColumns(TARGET_TABLE)

column_exists = any(
    column.name.lower() == COLUMN_NAME.lower()
    for column in columns
)

if column_exists:
    print(f"Column '{COLUMN_NAME}' already exists. Skipping migration.")
else:
    spark.sql(f"""
        ALTER TABLE {TARGET_TABLE}
        ADD COLUMNS (
            {COLUMN_NAME} STRING
        )
    """)

    print(f"Column '{COLUMN_NAME}' added successfully.")

print("Migration completed successfully.")