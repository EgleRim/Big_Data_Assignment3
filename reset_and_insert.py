import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import math

# 🔹 1. CONNECT TO MONGODB
client = MongoClient("mongodb://localhost:27017")
db = client["vesselDB"]
collection = db["data"]

print("Connected to MongoDB")

# 🔹 2. CLEAN DATABASE (DELETE ALL DOCUMENTS)
delete_result = collection.delete_many({})
print(f"Deleted {delete_result.deleted_count} existing documents")

# 🔹 3. LOAD CSV
df = pd.read_csv("aisdk-2026-04-18-test.csv")

print("CSV loaded. Rows:", len(df))

# 🔹 4. RENAME COLUMNS (MATCH YOUR SCHEMA)
df = df.rename(columns={
    "# Timestamp": "timestamp",
    "Type of mobile": "mobile_type",
    "Navigational status": "nav_status",
    "Ship type": "ship_type",
    "Cargo type": "cargo_type",
    "Type of position fixing device": "pos_device_type",
    "Data source type": "data_source_type"
})

# 🔹 5. TAKE FIRST 1000 ROWS
df = df.head(1000)

# 🔹 6. CLEAN + CONVERT DATA TYPES

def safe_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except:
        return None

def safe_int(x):
    try:
        if pd.isna(x):
            return None
        return int(x)
    except:
        return None

def safe_str(x):
    if pd.isna(x):
        return None
    return str(x)

def parse_date(x):
    try:
        return pd.to_datetime(x)
    except:
        return None

# Apply conversions
df["MMSI"] = df["MMSI"].apply(safe_int)
df["timestamp"] = df["timestamp"].apply(parse_date)

float_cols = ["Latitude", "Longitude", "ROT", "SOG", "COG", "Heading",
              "Width", "Length", "Draught", "A", "B", "C", "D"]

for col in float_cols:
    if col in df.columns:
        df[col] = df[col].apply(safe_float)

str_cols = ["mobile_type", "nav_status", "IMO", "Callsign", "Name",
            "ship_type", "cargo_type", "pos_device_type",
            "Destination", "ETA", "data_source_type"]

for col in str_cols:
    if col in df.columns:
        df[col] = df[col].apply(safe_str)

# 🔹 7. DROP INVALID ROWS (required fields)
df = df.dropna(subset=["MMSI", "timestamp", "Latitude", "Longitude"])

print("After cleaning:", len(df), "rows")

# 🔹 8. CONVERT TO DICTIONARY
data = df.to_dict(orient="records")

# 🔹 9. INSERT INTO MONGODB
if data:
    result = collection.insert_many(data, ordered=False)
    print("Inserted documents:", len(result.inserted_ids))
else:
    print("No valid data to insert")

# 🔹 10. VERIFY INSERTION
count = collection.count_documents({})
print("Documents in DB:", count)

# 🔹 11. SHOW SAMPLE
for doc in collection.find().limit(3):
    print(doc)