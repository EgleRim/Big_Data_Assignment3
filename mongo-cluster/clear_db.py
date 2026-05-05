from pymongo import MongoClient

client = MongoClient("mongodb://127.0.0.1:27017")
db = client["vesselDB"]

deleted = db.ais_data.delete_many({})
print(f"Deleted {deleted.deleted_count} documents")
print(f"Remaining: {db.ais_data.count_documents({})}")
