import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

CATALOG = "telematics"
SCHEMA = sys.argv[1]

# Number of seconds to generate
DURATION_SECONDS = int(sys.argv[2])

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# Unique ID for this execution
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

TRUCK_IDS = [f"TRK-{i:03d}" for i in range(1, 21)]

# Approximate starting locations
START_LOCATIONS = {
    "TRK-001": (41.8781, -87.6298),
    "TRK-002": (32.7767, -96.7970),
    "TRK-003": (39.7392, -104.9903),
    "TRK-004": (33.7490, -84.3880),
    "TRK-005": (41.8781, -87.6298),
    "TRK-006": (32.7767, -96.7970),
    "TRK-007": (39.7392, -104.9903),
    "TRK-008": (33.7490, -84.3880),
    "TRK-009": (41.8781, -87.6298),
    "TRK-010": (32.7767, -96.7970),
    "TRK-011": (39.7392, -104.9903),
    "TRK-012": (33.7490, -84.3880),
    "TRK-013": (41.8781, -87.6298),
    "TRK-014": (32.7767, -96.7970),
    "TRK-015": (39.7392, -104.9903),
    "TRK-016": (33.7490, -84.3880),
    "TRK-017": (41.8781, -87.6298),
    "TRK-018": (32.7767, -96.7970),
    "TRK-019": (39.7392, -104.9903),
    "TRK-020": (33.7490, -84.3880),
}

random.seed(42)

base_time = datetime.now(timezone.utc).replace(microsecond=0)

print("========================================")
print("GPS GENERATION STARTED")
print("========================================")
print(f"Schema   : {SCHEMA}")
print(f"Run ID   : {RUN_ID}")
print(f"Duration : {DURATION_SECONDS} seconds")
print(f"Landing  : {LANDING_PATH}")
print("========================================")


for second in range(DURATION_SECONDS):

    events = []

    current_time = base_time + timedelta(seconds=second)

    for truck_id in TRUCK_IDS:

        start_lat, start_lon = START_LOCATIONS[truck_id]

        lat = start_lat + random.uniform(-0.01, 0.01)
        lon = start_lon + random.uniform(-0.01, 0.01)

        speed = round(random.uniform(20, 100), 2)

        event_time = current_time

        event_id = str(uuid.uuid4())

        event = {
            "truck_id": truck_id,
            "event_ts": event_time.isoformat().replace("+00:00", "Z"),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "speed_kmh": speed,
            "event_id": event_id,
        }

        events.append(event)

    # ---------------------------------------------------------
    # Introduce malformed coordinates
    # ---------------------------------------------------------

    if second % 15 == 0:
        events[0]["lat"] = 125.0

    if second % 20 == 0:
        events[1]["lon"] = -250.0

    # ---------------------------------------------------------
    # Introduce duplicate event
    # ---------------------------------------------------------

    if second % 10 == 0:
        duplicate_event = events[2].copy()
        events.append(duplicate_event)

    # ---------------------------------------------------------
    # Introduce an out-of-order timestamp
    # ---------------------------------------------------------

    if second % 12 == 0 and second > 0:
        events[3]["event_ts"] = (
            current_time - timedelta(seconds=10)
        ).isoformat().replace("+00:00", "Z")

    # ---------------------------------------------------------
    # Shuffle records
    # ---------------------------------------------------------

    random.shuffle(events)

    # ---------------------------------------------------------
    # Unique filename for every pipeline execution
    # ---------------------------------------------------------

    filename = (
        f"{LANDING_PATH}/"
        f"gps_{RUN_ID}_{second:06d}.json"
    )

    content = json.dumps(events)

    dbutils.fs.put(
        filename,
        content,
        overwrite=False
    )

    print(
        f"Generated batch {second + 1}/{DURATION_SECONDS}: "
        f"{len(events)} events -> {filename}"
    )

    time.sleep(1)


print("========================================")
print("GPS GENERATION COMPLETED")
print("========================================")