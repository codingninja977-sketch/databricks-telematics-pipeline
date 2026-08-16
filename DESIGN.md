# Design Decisions

## 1. Architecture

The solution uses a Medallion architecture:

Landing -> Bronze -> Silver -> Gold

Synthetic GPS events are written as JSON files into a Unity Catalog managed
volume. Databricks Auto Loader ingests the files into Bronze. Silver performs
type normalization, deduplication, coordinate validation, quarantine, and
enrichment with static truck reference data. Gold produces a business-ready
daily truck aggregation.

The same Databricks Asset Bundle is deployed to DEV, TEST, and PROD targets.

---

## 2. Streaming Approach

The implementation uses hand-rolled Structured Streaming with Auto Loader
rather than Lakeflow Declarative Pipelines.

This approach keeps the implementation explicit and makes the ingestion,
checkpointing, foreachBatch processing, and Delta writes easy to demonstrate.

Auto Loader is used for file discovery so that newly arriving JSON files can
be processed incrementally without repeatedly scanning the entire landing
directory.

---

## 3. Bronze Layer

Bronze stays close to the raw source structure.

Auto Loader reads JSON files from:

`/Volumes/telematics/<schema>/landing`

and writes the raw events into:

`telematics.<schema>.bronze_gps_events`

A checkpoint is maintained under the landing volume so that the stream can
resume after a restart without reprocessing already committed files.

---

## 4. Silver Layer

Silver performs the main data-quality and conformance work.

The transformation includes:

- Type casting
- Timestamp normalization
- Coordinate validation
- Duplicate removal
- Static truck-details enrichment
- Invalid-record quarantine

Invalid GPS coordinates are written to:

`telematics.<schema>.quarantine_gps_events`

Quality metrics are written to:

`telematics.<schema>.gps_quality_metrics`

The Silver stream uses a checkpoint so processing can safely resume after
failure or restart.

---

## 5. Deduplication

GPS events contain an `event_id`, which is used as the event identity for
deduplication.

This prevents the same GPS event from being represented multiple times in the
Silver layer when duplicate source records are received.

The source generator intentionally introduces duplicate events to demonstrate
that the pipeline handles them.

---

## 6. Stream-Static Join

The Silver processing enriches GPS events with the static `truck_details`
reference table.

The reference table contains approximately 20 trucks and attributes such as:

- truck_id
- make
- model
- capacity_lbs
- home_depot
- region
- driver
- vehicle_type

The static side is read as a Delta table and joined with the streaming GPS
data using `truck_id`.

This is appropriate because the truck-details dataset is small relative to
the streaming GPS event volume.

---

## 7. Gold Layer

The Gold layer produces a business-ready truck-level daily aggregation.

The Gold output is:

`telematics.<schema>.truck_daily_summary`

This provides an analytics-friendly representation instead of exposing the
raw GPS event stream directly to consumers.

---

## 8. Idempotency and Restart Behavior

The GPS generator creates a unique run identifier for every execution so that
repeated executions create new landing files rather than attempting to
overwrite previous files.

Streaming ingestion uses checkpoints.

Silver and downstream writes are designed to be safe for reprocessing and
restart.

The schema migration is also idempotent. The migration checks whether the
`vehicle_type` column already exists before attempting to add it. If the
column is already present, the migration skips the ALTER TABLE operation and
completes successfully.

---

## 9. DEV / TEST / PROD

A single Asset Bundle contains all three targets.

The targets differ through configuration:

- DEV -> `telematics.dev`
- TEST -> `telematics.test`
- PROD -> `telematics.prod`

The same source code is deployed to each environment.

The schema is passed to Python jobs through the Bundle variable:

`${var.schema_name}`

This avoids maintaining separate copies of the pipeline for each environment.

DEV and TEST use development mode. PROD uses production mode and a dedicated
Bundle root path.

---

## 10. Deployment

Deployment is performed through the Databricks CLI:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev

databricks bundle validate -t test
databricks bundle deploy -t test

databricks bundle validate -t prod
databricks bundle deploy -t prod
---

## 11. Schema Migration

Schema changes are stored in the `migrations` directory.

The `002_add_vehicle_type.sql` migration adds the `vehicle_type` column to the
truck details table.

The migration is executed through the Bundle-managed
`migrate_truck_details` job.

The migration runner receives the environment schema from the Bundle and
operates on the corresponding table:

- DEV -> `telematics.dev.truck_details`
- TEST -> `telematics.test.truck_details`
- PROD -> `telematics.prod.truck_details`

The migration was promoted through DEV, TEST, and PROD without manually
executing an ALTER TABLE statement in PROD.

The migration runner is idempotent, so rerunning the migration after the
column has already been added completes successfully without attempting to add
the column again.

---

## 12. Table and Schema Management

Unity Catalog schemas and the landing volume are defined in the Asset Bundle.

Reference-table creation is performed programmatically through a
Bundle-managed job rather than manually creating the table in the production
UI.

The goal is to keep infrastructure and table structure reproducible from Git.

---

## 13. Production Discipline

PROD is treated as deploy-only.

Changes are made in source control and promoted through the Asset Bundle rather
than manually modifying production objects through the Databricks UI.

The demonstrated schema migration followed:

DEV -> TEST -> PROD

with validation, deployment, and migration execution performed through the
Bundle-managed job.

---

## 14. Scaling

The current implementation is intentionally small and suitable for the
take-home exercise.

For hundreds of thousands of trucks, the design could scale by:

- Increasing serverless compute as required
- Using efficient incremental processing
- Controlling state and checkpoint size
- Avoiding unnecessary shuffles
- Optimizing the static reference-data join
- Applying appropriate Delta optimization strategies
- Separating ingestion and transformation workloads when required

The core architecture remains:

Landing -> Bronze -> Silver -> Gold
