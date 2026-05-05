"""
Task 2: Parallel Data Insertion into MongoDB
=============================================
NO PANDAS in the data-loading path. Real big-data style:
  - csv.reader streams the file one line at a time
  - A generator yields validated rows (lazy, never holds the full file)
  - Chunks are batched and dispatched to parallel worker threads
  - Each worker creates its own MongoClient and bulk-inserts

Usage:
    python parallel_insert.py --csv aisdk-2026-04-18.csv --workers 4 --chunk-size 50000
"""

import argparse
import csv
import time
import threading
import uuid
from datetime import datetime
from typing import Generator, Iterator, List, Dict
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# ── Configuration ──────────────────────────────────────────────────────────────
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "vesselDB"
COLLECTION_NAME = "ais_data"

# ── CSV column indexes (Danish AIS format) ─────────────────────────────────────
# Header: # Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,
#         ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,
#         Width,Length,Draught,Destination,ETA
COL_TIMESTAMP   = 0
COL_MOBILE_TYPE = 1
COL_MMSI        = 2
COL_LATITUDE    = 3
COL_LONGITUDE   = 4
COL_NAV_STATUS  = 5
COL_ROT         = 6
COL_SOG         = 7
COL_COG         = 8
COL_HEADING     = 9
COL_IMO         = 10
COL_CALLSIGN    = 11
COL_NAME        = 12
COL_SHIP_TYPE   = 13
COL_CARGO_TYPE  = 14
COL_WIDTH       = 15
COL_LENGTH      = 16
COL_DRAUGHT     = 17
COL_DESTINATION = 18
COL_ETA         = 19

EXPECTED_MIN_COLS = 20


# ── Type-conversion helpers ────────────────────────────────────────────────────

def parse_float(s: str):
    """Return float or None if blank/unparseable."""
    s = s.strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_int(s: str):
    s = s.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_str(s: str):
    s = s.strip()
    return s if s else None


def parse_timestamp(s: str):
    """DD/MM/YYYY HH:MM:SS -> datetime, or None."""
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y %H:%M:%S")
    except (ValueError, TypeError):
        return None


# ── Streaming generator (no pandas, low memory) ────────────────────────────────

def stream_valid_records(filepath: str, limit: int = None) -> Generator[Dict, None, None]:
    """
    Generator: opens the CSV, skips the header, and yields one cleaned dict per
    valid row. Reads the file line-by-line — at no point is the full file in RAM.

    Filters dropped rows where required fields are missing/invalid:
      - MMSI must parse as int
      - timestamp must parse
      - latitude and longitude must parse as floats
    """
    yielded = 0
    with open(filepath, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header

        for row in reader:
            # Skip malformed/truncated lines
            if len(row) < EXPECTED_MIN_COLS:
                continue

            mmsi      = parse_int(row[COL_MMSI])
            timestamp = parse_timestamp(row[COL_TIMESTAMP])
            latitude  = parse_float(row[COL_LATITUDE])
            longitude = parse_float(row[COL_LONGITUDE])

            # Required fields must be present
            if mmsi is None or timestamp is None or latitude is None or longitude is None:
                continue

            # Build the document
            doc = {
                "_id":         str(uuid.uuid4()),  # guaranteed-unique to avoid worker collisions
                "timestamp":   timestamp,           # BSON Date, not string
                "mobile_type": parse_str(row[COL_MOBILE_TYPE]),
                "MMSI":        mmsi,
                "Latitude":    latitude,
                "Longitude":   longitude,
                "nav_status":  parse_str(row[COL_NAV_STATUS]),
                "ROT":         parse_float(row[COL_ROT]),
                "SOG":         parse_float(row[COL_SOG]),
                "COG":         parse_float(row[COL_COG]),
                "Heading":     parse_float(row[COL_HEADING]),
                "IMO":         parse_str(row[COL_IMO]),
                "Callsign":    parse_str(row[COL_CALLSIGN]),
                "Name":        parse_str(row[COL_NAME]),
                "ship_type":   parse_str(row[COL_SHIP_TYPE]),
                "cargo_type":  parse_str(row[COL_CARGO_TYPE]),
                "Width":       parse_float(row[COL_WIDTH]),
                "Length":      parse_float(row[COL_LENGTH]),
                "Draught":     parse_float(row[COL_DRAUGHT]),
                "Destination": parse_str(row[COL_DESTINATION]),
                "ETA":         parse_str(row[COL_ETA]),
            }

            yield doc
            yielded += 1
            if limit is not None and yielded >= limit:
                return


def chunked(iterator: Iterator[Dict], size: int) -> Generator[List[Dict], None, None]:
    """
    Batch the streamed records into lists of `size` dicts.
    Pure generator — only one chunk in memory at a time.
    """
    chunk = []
    for item in iterator:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


# ── Worker function (one MongoClient per thread) ───────────────────────────────

def insert_chunk(worker_id: int, docs: List[Dict], results: dict, lock: threading.Lock):
    t_start = time.perf_counter()

    # Each worker gets its OWN MongoClient — required by the assignment
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    collection = client[DB_NAME][COLLECTION_NAME]

    inserted = 0
    errors   = 0

    if docs:
        try:
            result = collection.insert_many(docs, ordered=False)
            inserted = len(result.inserted_ids)
        except BulkWriteError as bwe:
            inserted = bwe.details.get("nInserted", 0)
            errors   = len(bwe.details.get("writeErrors", []))
        except Exception as e:
            print(f"[Worker {worker_id}] Fatal error: {e}")
            errors = len(docs)

    client.close()
    elapsed = time.perf_counter() - t_start

    with lock:
        results[worker_id] = {"inserted": inserted, "errors": errors, "elapsed": round(elapsed, 2)}

    print(f"[Worker {worker_id:>3}] inserted={inserted:>6}  errors={errors:>4}  time={elapsed:.2f}s")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Parallel AIS data insertion (no-pandas streaming)")
    parser.add_argument("--csv",        default="aisdk-2026-04-18.csv")
    parser.add_argument("--workers",    type=int, default=4)
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--limit",      type=int, default=None)
    args = parser.parse_args()

    print("=" * 60)
    print("AIS Parallel Inserter (csv.reader + generators)")
    print(f"  CSV:        {args.csv}")
    print(f"  Workers:    {args.workers}")
    print(f"  Chunk size: {args.chunk_size:,}")
    print(f"  Row limit:  {args.limit or 'all'}")
    print(f"  MongoDB:    {MONGO_URI}  ->  {DB_NAME}.{COLLECTION_NAME}")
    print("=" * 60)

    # ── Step 1: Create indexes ──────────────────────────────────────────────
    print("\nCreating indexes...")
    setup_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    col = setup_client[DB_NAME][COLLECTION_NAME]
    col.create_index("MMSI")
    col.create_index("timestamp")
    col.create_index([("MMSI", 1), ("timestamp", 1)])
    setup_client.close()
    print("  Indexes ready.")

    # ── Step 2: Stream + chunk + dispatch in parallel ────────────────────────
    print(f"\nStreaming CSV and inserting ({args.workers} chunks at a time)...\n")

    record_stream = stream_valid_records(args.csv, limit=args.limit)
    chunk_stream  = chunked(record_stream, args.chunk_size)

    results = {}
    lock = threading.Lock()
    chunk_idx = 0
    total_inserted = 0
    total_errors = 0
    t_start = time.perf_counter()

    while True:
        # Pull next `workers` chunks from the lazy stream
        batch = []
        for _ in range(args.workers):
            try:
                batch.append(next(chunk_stream))
            except StopIteration:
                break

        if not batch:
            break

        # Launch one thread per chunk
        threads = []
        for i, chunk in enumerate(batch):
            worker_id = chunk_idx + i
            t = threading.Thread(
                target=insert_chunk,
                args=(worker_id, chunk, results, lock),
                daemon=True,
            )
            threads.append(t)
            t.start()

        # Wait for all threads in this batch to finish before pulling next batch
        for t in threads:
            t.join()

        # Tally results and free memory
        for i in range(len(batch)):
            wid = chunk_idx + i
            if wid in results:
                total_inserted += results[wid]["inserted"]
                total_errors   += results[wid]["errors"]
                del results[wid]

        chunk_idx += len(batch)
        elapsed = time.perf_counter() - t_start
        print(f"  --> Chunks done: {chunk_idx}  |  Inserted: {total_inserted:,}  |  Elapsed: {elapsed:.0f}s\n")

    total_elapsed = time.perf_counter() - t_start

    print("=" * 60)
    print("INSERTION COMPLETE")
    print(f"  Total inserted   : {total_inserted:,}")
    print(f"  Total errors     : {total_errors:,}")
    print(f"  Total time       : {total_elapsed:.2f}s")
    if total_elapsed > 0:
        print(f"  Throughput       : {total_inserted / total_elapsed:,.0f} docs/sec")
    print("=" * 60)

    # Verify directly in DB
    verify_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    db_count = verify_client[DB_NAME][COLLECTION_NAME].count_documents({})
    verify_client.close()
    print(f"\nDocuments now in {DB_NAME}.{COLLECTION_NAME}: {db_count:,}")


if __name__ == "__main__":
    main()
