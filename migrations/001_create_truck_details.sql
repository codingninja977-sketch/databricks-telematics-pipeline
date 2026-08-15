CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.truck_details (
    truck_id STRING NOT NULL,
    make STRING,
    model STRING,
    capacity_lbs BIGINT,
    home_depot STRING,
    region STRING,
    driver STRING
);