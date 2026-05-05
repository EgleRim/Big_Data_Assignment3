import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# === CONFIGURATION ===
CSV_FILE_PATH = "aisdk-2026-04-18.csv"
SAMPLE_ROWS = 500

# === READ SAMPLE (only reads first 500 rows) ===
df = pd.read_csv(CSV_FILE_PATH, nrows=SAMPLE_ROWS)

# === BASIC STRUCTURE ===
print("=" * 60)
print(f"COLUMNS ({len(df.columns)}):")
print("=" * 60)
print(df.columns.tolist())

print("\n" + "=" * 60)
print("DATA TYPES:")
print("=" * 60)
print(df.dtypes)

print("\n" + "=" * 60)
print("FIRST 5 ROWS:")
print("=" * 60)
print(df.head())

print("\n" + "=" * 60)
print("SHAPE:", df.shape)
print("=" * 60)

# === STATS USEFUL FOR NoSQL SCHEMA DESIGN ===
print("\n" + "=" * 60)
print("NULL COUNTS (per column):")
print("=" * 60)
print(df.isnull().sum())

print("\n" + "=" * 60)
print("UNIQUE VALUE COUNTS (per column):")
print("=" * 60)
print(df.nunique())

print("\n" + "=" * 60)
print("SAMPLE VALUES (first non-null per column):")
print("=" * 60)
for col in df.columns:
    sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
    print(f"  {col}: {repr(sample_val)} (type: {type(sample_val).__name__})")

# === NoSQL CONSIDERATIONS ===
print("\n" + "=" * 60)
print("NoSQL DESIGN NOTES:")
print("=" * 60)

# Identify potential document keys / partition keys
# Columns with high cardinality = good candidate for document ID / partition key
# Columns with low cardinality = good for indexing / grouping
high_cardinality = df.nunique()[df.nunique() > SAMPLE_ROWS * 0.8].index.tolist()
low_cardinality = df.nunique()[df.nunique() < 20].index.tolist()

print(f"  Potential ID/partition key candidates (high cardinality): {high_cardinality}")
print(f"  Potential grouping/index fields (low cardinality): {low_cardinality}")

# Detect columns that might contain nested/list data (e.g., comma-separated values)
print("\n  Columns possibly containing nested/list data:")
for col in df.select_dtypes(include="object").columns:
    sample = df[col].dropna().head(20)
    has_lists = sample.str.contains(r"[,;|\[\{]", na=False).any()
    if has_lists:
        print(f"    - {col} (may need embedding as sub-document/array)")

print("\n" + "=" * 60)
print("MEMORY USAGE OF SAMPLE:")
print("=" * 60)
print(df.memory_usage(deep=True))


# create a test file
import pandas as pd

# === CONFIGURATION ===
CSV_FILE_PATH = "aisdk-2026-04-18.csv"
SAMPLE_ROWS = 500  # 500 rows is a good balance: enough to detect patterns, fast to load

# === READ SAMPLE (memory-efficient, only reads first N rows) ===
df = pd.read_csv(CSV_FILE_PATH, nrows=SAMPLE_ROWS)

# === BASIC STRUCTURE ===
print("=" * 60)
print(f"COLUMNS ({len(df.columns)}):")
print("=" * 60)
print(df.columns.tolist())

print("\n" + "=" * 60)
print("DATA TYPES:")
print("=" * 60)
print(df.dtypes)

print("\n" + "=" * 60)
print("FIRST 5 ROWS:")
print("=" * 60)
print(df.head())

print("\n" + "=" * 60)
print("SHAPE:", df.shape)
print("=" * 60)

# === STATS USEFUL FOR NoSQL SCHEMA DESIGN ===
print("\n" + "=" * 60)
print("NULL COUNTS (per column):")
print("=" * 60)
print(df.isnull().sum())

print("\n" + "=" * 60)
print("UNIQUE VALUE COUNTS (per column):")
print("=" * 60)
print(df.nunique())

print("\n" + "=" * 60)
print("SAMPLE VALUES (first non-null per column):")
print("=" * 60)
for col in df.columns:
    sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
    print(f"  {col}: {repr(sample_val)} (type: {type(sample_val).__name__})")

# === NoSQL CONSIDERATIONS ===
print("\n" + "=" * 60)
print("NoSQL DESIGN NOTES:")
print("=" * 60)

# Identify potential document keys / partition keys
# Columns with high cardinality = good candidate for document ID / partition key
# Columns with low cardinality = good for indexing / grouping
high_cardinality = df.nunique()[df.nunique() > SAMPLE_ROWS * 0.8].index.tolist()
low_cardinality = df.nunique()[df.nunique() < 20].index.tolist()

print(f"  Potential ID/partition key candidates (high cardinality): {high_cardinality}")
print(f"  Potential grouping/index fields (low cardinality): {low_cardinality}")

# Detect columns that might contain nested/list data (e.g., comma-separated values)
print("\n  Columns possibly containing nested/list data:")
for col in df.select_dtypes(include="object").columns:
    sample = df[col].dropna().head(20)
    has_lists = sample.str.contains(r"[,;|\[\{]", na=False).any()
    if has_lists:
        print(f"    - {col} (may need embedding as sub-document/array)")

print("\n" + "=" * 60)
print("MEMORY USAGE OF SAMPLE:")
print("=" * 60)
print(df.memory_usage(deep=True))


# CREATE A TEST CSV

np.random.seed(42)

# 10 vessels, each with ~100 position reports
mmsi_list = [219000001 + i for i in range(10)]
rows = []

for mmsi in mmsi_list:
    # Each vessel starts at a random time on 2026-04-18
    start_time = datetime(2026, 4, 18, np.random.randint(0, 12), np.random.randint(0, 60))
    lat = 55.0 + np.random.uniform(-1, 1)  # Around Denmark
    lon = 11.0 + np.random.uniform(-1, 1)

    for j in range(100):
        # Timestamps every 5-30 seconds (typical AIS frequency)
        ts = start_time + timedelta(seconds=int(j * np.random.uniform(5, 30)))
        lat += np.random.uniform(-0.001, 0.001)
        lon += np.random.uniform(-0.001, 0.001)

        rows.append({
            "# Timestamp": ts.strftime("%d/%m/%Y %H:%M:%S"),
            "Type of mobile": np.random.choice(["Class A", "Class B", "Base Station"]),
            "MMSI": mmsi,
            "Latitude": round(lat, 6),
            "Longitude": round(lon, 6),
            "Navigational status": np.random.choice(["Under way using engine", "At anchor", "Moored", None]),
            "ROT": round(np.random.uniform(-5, 5), 1),
            "SOG": round(np.random.uniform(0, 15), 1),
            "COG": round(np.random.uniform(0, 360), 1),
            "Heading": np.random.randint(0, 360),
            "IMO": np.random.choice([None, f"IMO{np.random.randint(9000000, 9999999)}"]),
            "Callsign": f"OX{np.random.choice(['AB','CD','EF','GH'])}{np.random.randint(1,9)}",
            "Name": np.random.choice(["MAERSK OSLO", "NORDIC WAVE", "BALTIC STAR", "COPENHAGEN EXPRESS", None]),
            "Ship type": np.random.choice(["Cargo", "Tanker", "Passenger", "Fishing", "Undefined"]),
            "Cargo type": np.random.choice(["No additional info", "Category A", None]),
            "Width": np.random.choice([10, 15, 20, 30, None]),
            "Length": np.random.choice([50, 100, 150, 200, None]),
            "Draught": round(np.random.uniform(3, 12), 1),
            "Destination": np.random.choice(["COPENHAGEN", "AARHUS", "HAMBURG", None]),
            "ETA": np.random.choice([None, "18/04/2026 18:00:00", "19/04/2026 06:00:00"]),
        })

df = pd.DataFrame(rows)
df.to_csv("aisdk-2026-04-18-test.csv", index=False)
print(f"Created test CSV with {len(df)} rows, {df['MMSI'].nunique()} vessels")
print(df.head())

