from pymongo import MongoClient, ASCENDING
from multiprocessing import Process

MONGO_URI = "mongodb://localhost:27017"

DB_NAME = "ais_db"
RAW_COLLECTION = "raw_data"
FILTERED_COLLECTION = "filtered_data"

NUM_WORKERS = 4
BATCH_SIZE = 5000


def is_valid(doc, invalid_vessels):
    """
    Checks whether a document is valid according to assignment rules.
    """

    # Remove vessels with <100 points
    if doc.get("MMSI") in invalid_vessels:
        return False

    # Required fields
    required_fields = [
        "MMSI",
        "Latitude",
        "Longitude",
        "ROT",
        "SOG",
        "COG",
        "Heading",
        "Navigational status",
        "BaseDateTime"
    ]

    # Missing or empty fields
    for field in required_fields:
        if field not in doc:
            return False

        if doc[field] is None:
            return False

        if str(doc[field]).strip() == "":
            return False

    # Basic coordinate validation
    try:
        lat = float(doc["Latitude"])
        lon = float(doc["Longitude"])

        if lat < -90 or lat > 90:
            return False

        if lon < -180 or lon > 180:
            return False

    except:
        return False

    return True


def worker(worker_id, invalid_vessels):

    client = MongoClient(MONGO_URI)

    db = client[DB_NAME]

    raw = db[RAW_COLLECTION]
    filtered = db[FILTERED_COLLECTION]

    batch = []
    inserted = 0
    checked = 0

    print(f"Worker {worker_id} started")

    # Split collection across workers
    cursor = raw.find({
        "_id": {
            "$mod": [NUM_WORKERS, worker_id]
        }
    })

    for doc in cursor:

        checked += 1

        if is_valid(doc, invalid_vessels):

            batch.append(doc)
            inserted += 1

            if len(batch) >= BATCH_SIZE:
                filtered.insert_many(batch)
                batch = []

        if checked % 50000 == 0:
            print(f"Worker {worker_id}: checked {checked}")

    # Insert remaining documents
    if batch:
        filtered.insert_many(batch)

    print(f"Worker {worker_id} DONE")
    print(f"Worker {worker_id} inserted: {inserted}")


def main():

    client = MongoClient(MONGO_URI)

    db = client[DB_NAME]

    raw = db[RAW_COLLECTION]
    filtered = db[FILTERED_COLLECTION]

    print("CREATING INDEXES...")

    # Helpful indexes for filtering and analysis
    raw.create_index([("MMSI", ASCENDING)])
    raw.create_index([("BaseDateTime", ASCENDING)])

    filtered.create_index([("MMSI", ASCENDING)])
    filtered.create_index([("BaseDateTime", ASCENDING)])

    print("FINDING INVALID VESSELS (<100 points)...")

    invalid_vessels = set(
        r["_id"] for r in raw.aggregate([
            {
                "$group": {
                    "_id": "$MMSI",
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "count": {"$lt": 100}
                }
            }
        ])
    )

    print("Invalid vessels:", len(invalid_vessels))

    processes = []

    print("STARTING PARALLEL FILTERING...")

    for i in range(NUM_WORKERS):

        p = Process(
            target=worker,
            args=(i, invalid_vessels)
        )

        p.start()

        processes.append(p)

    for p in processes:
        p.join()

    print("FILTERING COMPLETE")

    print(
        "Filtered rows:",
        filtered.count_documents({})
    )


if __name__ == "__main__":
    main()
