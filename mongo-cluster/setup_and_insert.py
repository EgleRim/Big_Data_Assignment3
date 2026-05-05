from pymongo import MongoClient
import random
import numpy as np
from datetime import datetime, timedelta

client = MongoClient("mongodb://127.0.0.1:27017")
db = client["vesselDB"]

# Enable sharding on the database
client.admin.command("enableSharding", "vesselDB")

# Create collection and shard it by MMSI (hashed for even distribution)
client.admin.command("shardCollection", "vesselDB.ais_data", key={"MMSI": "hashed"})

# Generate 1000 test documents
np.random.seed(42)
mmsi_list = [219000001 + i for i in range(10)]
docs = []

for i in range(1000):
    mmsi = random.choice(mmsi_list)
    ts = datetime(2026, 4, 18, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    docs.append({
        "Timestamp": ts,
        "Type_of_mobile": random.choice(["Class A", "Class B", "Base Station"]),
        "MMSI": mmsi,
        "Latitude": round(55.0 + random.uniform(-1, 1), 6),
        "Longitude": round(11.0 + random.uniform(-1, 1), 6),
        "Navigational_status": random.choice(["Under way using engine", "At anchor", "Moored", None]),
        "ROT": round(random.uniform(-5, 5), 1),
        "SOG": round(random.uniform(0, 15), 1),
        "COG": round(random.uniform(0, 360), 1),
        "Heading": random.randint(0, 360),
        "Ship_type": random.choice(["Cargo", "Tanker", "Passenger", "Fishing"]),
        "Destination": random.choice(["COPENHAGEN", "AARHUS", "HAMBURG", None]),
    })

result = db.ais_data.insert_many(docs)
print(f"Inserted {len(result.inserted_ids)} documents")

# Verify
print(f"Total documents in collection: {db.ais_data.count_documents({})}")
print(f"Distribution check:")
for shard_doc in client.admin.command("collStats", "ais_data", **{"$db": "vesselDB"})["shards"].items():
    print(f"  {shard_doc[0]}: {shard_doc[1]['count']} docs")
