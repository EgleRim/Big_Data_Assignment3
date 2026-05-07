"""
Task 3: Parallel Data Noise Filtering
======================================
Reads from vesselDB.ais_data (inserted by Task 2),
applies noise filters in parallel (one thread per MMSI batch),
and writes clean records to vesselDB.ais_filtered.

Five categories of noise are removed:

  Category 1 — Invalid MMSI numbers:
    - Known bad patterns: 000000000, 111111111, 123456789, 987654321, etc.
    - All-same-digit MMSIs (111111111, 222222222, ..., 999999999)
    - Wrong length (not exactly 9 digits) or non-numeric
    These come from unconfigured transponders and would create a single
    massive MMSI bucket crashing worker memory.

  Category 2 — Invalid coordinates:
    - Latitude outside [-90, 90] or Longitude outside [-180, 180]
    - Exactly (0.0, 0.0) — "Null Island", AIS default when GPS not locked
    AIS devices report 0°N 0°E when position is unavailable; including
    these would generate false teleportation events.

  Category 3 — Malformed / truncated rows:
    - Timestamp field missing or not parseable
    These arise from partial writes, encoding errors, or CSV corruption.

  Category 4 — AIS base stations and non-vessel transponders:
    - MMSI prefix 992xxxxxxx = shore-based AIS base stations
    - MMSI prefix 970xxxxxxx = AIS SART (search-and-rescue transmitters)
    - MMSI prefix 111xxxxxxx = SAR aircraft
    These never have draught, SOG, or movement data; treating them as
    vessels would generate false detection events.

  Category 5 — Vessels with too few data points:
    - Vessels with < 100 records are dropped entirely.

  Category 6 — Records missing required fields
    (MMSI, Latitude, Longitude, ROT, SOG, COG, Heading, nav_status).

Usage:
    python noise_filtering.py --workers 4 --batch-size 20
"""

import argparse
import math
import threading
import time
from datetime import datetime
from typing import List

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

# ── Configuration ──────────────────────────────────────────────────────────────
MONGO_URI           = "mongodb://localhost:27017"
DB_NAME             = "vesselDB"
SOURCE_COLLECTION   = "ais_data"
FILTERED_COLLECTION = "ais_filtered"

MIN_DATAPOINTS = 100   #vessels with fewer records are noise

# ── MMSI validation constants  ────────────────────────────────────────────────
EXPECTED_MMSI_LENGTH = 9

# Hard-coded known-bad transponder IDs (unconfigured / test devices)
INVALID_MMSI_PATTERNS = {
    "000000000",
    "123456789",
    "987654321",
    "111111111",
    "222222222",
    "333333333",
    "444444444",
    "555555555",
    "666666666",
    "777777777",
    "888888888",
    "999999999",
}

# MMSI prefixes identifying non-vessel stations
#   992xxxxxx = AIS base stations
#   970xxxxxx = AIS SART (search-and-rescue transmitters)
#   111xxxxxx = SAR aircraft
INVALID_MMSI_PREFIXES = ("992", "970", "111")

# Fields required to be non-null for a record
REQUIRED_FIELDS = [
    "MMSI", "Latitude", "Longitude",
    "ROT", "SOG", "COG", "Heading", "nav_status"
]

# Subset of required fields that must also be finite numbers
NUMERIC_FIELDS = ["Latitude", "Longitude", "ROT", "SOG", "COG", "Heading"]

# Timestamp formats to try when the field is a raw string
TIMESTAMP_FORMATS = [
    "%d/%m/%Y %H:%M:%S",   # Danish AIS CSV format
    "%Y-%m-%d %H:%M:%S",   # ISO format (common after pandas import)
    "%Y-%m-%dT%H:%M:%S",   # ISO 8601
]


# ── MMSI validation ───────────────────────────────────────────

def is_valid_mmsi(mmsi):
    """
    Validate MMSI against all known dirty-data patterns.
    """
    s = str(mmsi).strip() if mmsi is not None else ""

    if not s or not s.isdigit():
        return False
    if len(s) != EXPECTED_MMSI_LENGTH:
        return False
    if s in INVALID_MMSI_PATTERNS:
        return False
    if s.startswith(INVALID_MMSI_PREFIXES):
        return False
    if len(set(s)) == 1:   # catches 000000000 through 999999999 uniformly
        return False

    return True


# ──  Coordinate validation ─────────────────────────────────────────

def is_valid_coordinate(lat, lon):
    try:
        flat = float(lat)
        flon = float(lon)
    except (TypeError, ValueError):
        return False

    if math.isnan(flat) or math.isnan(flon):
        return False
    if not (-90.0 <= flat <= 90.0):
        return False
    if not (-180.0 <= flon <= 180.0):
        return False
    if flat == 0.0 and flon == 0.0:
        return False   # Null Island

    return True


# ── Timestamp validation ──────────────────────────────────────────

def is_valid_timestamp(ts):

    if ts is None:
        return False
    if isinstance(ts, datetime):
        return True
    if isinstance(ts, str):
        ts = ts.strip()
        if not ts:
            return False
        for fmt in TIMESTAMP_FORMATS:
            try:
                datetime.strptime(ts, fmt)
                return True
            except ValueError:
                continue
        return False
    return False


# ── Combined per-record validator ──────────────────────────────────────────────

def is_valid_record(doc: dict):
    """
    Returns True only if the document passes ALL filter categories:

    """
    # ── Baseline: required fields present and non-empty ─────────────────────
    for field in REQUIRED_FIELDS:
        val = doc.get(field)
        if val is None:
            return False
        if isinstance(val, str) and val.strip() == "":
            return False

    # ── Numeric fields must be finite ────────────────────────────────────────
    for field in NUMERIC_FIELDS:
        try:
            fval = float(doc[field])
        except (TypeError, ValueError):
            return False
        if math.isnan(fval) or math.isinf(fval):
            return False

    # ── MMSI ────────────────────────────────────────────────
    if not is_valid_mmsi(doc.get("MMSI")):
        return False

    # ── Coordinates ──────────────────────────────────────────────
    if not is_valid_coordinate(doc.get("Latitude"), doc.get("Longitude")):
        return False

    # ── Timestamp ────────────────────────────────────────────────
    if not is_valid_timestamp(doc.get("timestamp")):
        return False

    return True


# ── Worker ─────────────────────────────────────────────────────────────────────

def filter_mmsi_batch(
    worker_id: int,
    mmsi_batch: List,
    results: dict,
    lock: threading.Lock,
):
    """
    One worker thread. Opens its own MongoClient.

    For each MMSI in the batch:
      a) Pre-filter the MMSI itself.
      b) Count check — drop entire vessel if < 100.
      c) Fetch all records and validate each with is_valid_record().
      d) Bulk-insert surviving records into ais_filtered.
    """
    t_start = time.perf_counter()
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
    src = client[DB_NAME][SOURCE_COLLECTION]
    dst = client[DB_NAME][FILTERED_COLLECTION]

    kept             = 0
    dropped_vessels  = 0
    dropped_records  = 0

    for mmsi in mmsi_batch:
        # ── a) MMSI check ──────────────────────────────
        if not is_valid_mmsi(mmsi):
            dropped_vessels += 1
            print(f"[Worker {worker_id:>2}] MMSI {mmsi}: invalid pattern → dropped (Cat 1/4)")
            continue

        # ── b) Data-point count ─────────────────────────────────
        count = src.count_documents({"MMSI": mmsi})
        if count < MIN_DATAPOINTS:
            dropped_vessels += 1
            print(f"[Worker {worker_id:>2}] MMSI {mmsi}: {count} pts < {MIN_DATAPOINTS} → dropped (Cat 5)")
            continue

        # ── c) Fetch + per-record validation ─────────────────────────────────
        valid_docs = []
        for doc in src.find({"MMSI": mmsi}):
            doc.pop("_id", None)   # fresh _id in filtered collection
            if is_valid_record(doc):
                valid_docs.append(doc)
            else:
                dropped_records += 1

        # ── d) Bulk insert ────────────────────────────────────────────────────
        if valid_docs:
            try:
                dst.insert_many(valid_docs, ordered=False)
                kept += len(valid_docs)
            except BulkWriteError as bwe:
                n = bwe.details.get("nInserted", 0)
                kept += n
                dropped_records += len(valid_docs) - n

    client.close()
    elapsed = time.perf_counter() - t_start

    with lock:
        results[worker_id] = {
            "kept":            kept,
            "dropped_vessels": dropped_vessels,
            "dropped_records": dropped_records,
            "elapsed":         round(elapsed, 2),
        }

    print(
        f"[Worker {worker_id:>2}] "
        f"kept={kept:>6}  "
        f"dropped_vessels={dropped_vessels:>3}  "
        f"dropped_records={dropped_records:>6}  "
        f"time={elapsed:.2f}s"
    )


# ── Index setup ────────────────────────────────────────────────────────────────

def create_indexes():

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)

    src = client[DB_NAME][SOURCE_COLLECTION]
    src.create_index([("MMSI", ASCENDING)],                           name="idx_mmsi")
    src.create_index([("MMSI", ASCENDING), ("timestamp", ASCENDING)], name="idx_mmsi_ts")

    dst = client[DB_NAME][FILTERED_COLLECTION]
    dst.create_index([("MMSI", ASCENDING)],                           name="idx_mmsi")
    dst.create_index([("MMSI", ASCENDING), ("timestamp", ASCENDING)], name="idx_mmsi_ts")
    dst.create_index([("nav_status", ASCENDING)],                     name="idx_nav_status")
    dst.create_index([("Latitude", ASCENDING), ("Longitude", ASCENDING)], name="idx_coords")

    client.close()
    print("  Indexes created on both collections.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Parallel AIS noise filtering")
    parser.add_argument("--workers",    type=int, default=4,  help="Number of parallel threads")
    parser.add_argument("--batch-size", type=int, default=20, help="MMSIs per worker batch")
    args = parser.parse_args()

    print("=" * 65)
    print("AIS Noise Filter  (parallel, per-MMSI)")
    print(f"  Source      : {DB_NAME}.{SOURCE_COLLECTION}")
    print(f"  Destination : {DB_NAME}.{FILTERED_COLLECTION}")
    print(f"  Min points  : {MIN_DATAPOINTS}")
    print(f"  Workers     : {args.workers}")
    print(f"  Batch size  : {args.batch_size} MMSIs/worker")
    print("=" * 65)


#Drop old indexes if present
    print("\nDropping old indexes...")
    drop_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
    drop_client[DB_NAME][SOURCE_COLLECTION].drop_indexes()
    drop_client[DB_NAME][FILTERED_COLLECTION].drop_indexes()
    drop_client.close()

    # ── 1. Indexes ───────────────────────────────────────────────────────────
    print("\nSetting up indexes...")
    create_indexes()

    # ── 2. Discover all distinct MMSIs ───────────────────────────────────────
    print("\nFetching distinct MMSIs from source collection...")
    setup_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
    all_mmsi     = setup_client[DB_NAME][SOURCE_COLLECTION].distinct("MMSI")
    total_source = setup_client[DB_NAME][SOURCE_COLLECTION].count_documents({})
    setup_client.close()

    print(f"  Found {len(all_mmsi):,} distinct vessels  |  {total_source:,} total source records")

    if not all_mmsi:
        print("\nNo data found in source collection. Run Task 2 first.")
        return

    # ── 3. Partition MMSIs into worker batches ───────────────────────────────
    batches = [
        all_mmsi[i : i + args.batch_size]
        for i in range(0, len(all_mmsi), args.batch_size)
    ]
    print(f"  Split into {len(batches)} batches of up to {args.batch_size} MMSIs each\n")

    # ── 4. Run workers in parallel ───────────────────────────────────────────
    results:       dict               = {}
    lock                              = threading.Lock()
    t_start                           = time.perf_counter()
    active_threads: List[threading.Thread] = []
    batch_idx                         = 0

    while batch_idx < len(batches):
        while len(active_threads) < args.workers and batch_idx < len(batches):
            t = threading.Thread(
                target=filter_mmsi_batch,
                args=(batch_idx, batches[batch_idx], results, lock),
                daemon=True,
            )
            active_threads.append(t)
            t.start()
            batch_idx += 1

        finished = [t for t in active_threads if not t.is_alive()]
        for t in finished:
            active_threads.remove(t)

        if len(active_threads) >= args.workers:
            active_threads[0].join()
            active_threads.pop(0)

    for t in active_threads:
        t.join()

    total_elapsed = time.perf_counter() - t_start

    # ── 5. Summary ───────────────────────────────────────────────────────────
    total_kept            = sum(r["kept"]            for r in results.values())
    total_dropped_vessels = sum(r["dropped_vessels"] for r in results.values())
    total_dropped_records = sum(r["dropped_records"] for r in results.values())

    verify_client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
    filtered_count   = verify_client[DB_NAME][FILTERED_COLLECTION].count_documents({})
    filtered_vessels = len(verify_client[DB_NAME][FILTERED_COLLECTION].distinct("MMSI"))
    verify_client.close()

    print("\n" + "=" * 65)
    print("FILTERING COMPLETE")
    print(f"  Source records        : {total_source:,}")
    print(f"  Vessels dropped       : {total_dropped_vessels:,}  (invalid MMSI or < {MIN_DATAPOINTS} pts)")
    print(f"  Records dropped       : {total_dropped_records:,}  (coord / timestamp / field checks)")
    print(f"  Records kept          : {total_kept:,}")
    print(f"  DB verify (filtered)  : {filtered_count:,} records  |  {filtered_vessels:,} vessels")
    print(f"  Total time            : {total_elapsed:.2f}s")
    print("=" * 65)


if __name__ == "__main__":
    main()