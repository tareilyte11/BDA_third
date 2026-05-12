from concurrent.futures import ProcessPoolExecutor, as_completed

from pymongo import MongoClient, ASCENDING

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"

DATABASE_NAME = "main-db"

MAIN_COLLECTION = "AIS_information"

# New filtered collection
FILTERED_COLLECTION = "filtered_AIS_information"

BATCH_SIZE = 10000
MMSI_QUERY_CHUNK_SIZE = 1000
MIN_RECORDS_PER_MMSI = 100
NUM_WORKERS = 4


REQUIRED_FIELDS = [
    "Navigational status",
    "MMSI",
    "Latitude",
    "Longitude",
    "ROT",
    "SOG",
    "COG",
    "Heading",
]


def _to_number(field, number_type):
    return {
        "$convert": {
            "input": f"${field}",
            "to": number_type,
            "onError": None,
            "onNull": None,
        }
    }


def build_validation_stages():
    required_field_filter = {}

    for field in REQUIRED_FIELDS:
        required_field_filter[field] = {
            "$exists": True,
            "$nin": [None, ""]
        }

    return [
        {"$match": required_field_filter},
        {
            "$addFields": {
                "__mmsi_num": _to_number("MMSI", "long"),
                "__lat_num": _to_number("Latitude", "double"),
                "__lon_num": _to_number("Longitude", "double"),
                "__rot_num": _to_number("ROT", "double"),
                "__sog_num": _to_number("SOG", "double"),
                "__cog_num": _to_number("COG", "double"),
                "__heading_num": _to_number("Heading", "long"),
            }
        },
        {
            "$match": {
                "__mmsi_num": {"$gt": 0},
                "__lat_num": {"$gte": -90, "$lte": 90},
                "__lon_num": {"$gte": -180, "$lte": 180},
                "__rot_num": {"$ne": None},
                "__sog_num": {"$gte": 0},
                "__cog_num": {"$gte": 0, "$lte": 360},
                "__heading_num": {"$gte": 0, "$lte": 511},
            }
        },
    ]


def build_valid_mmsi_pipeline():

    return [
        *build_validation_stages(),
        {
            "$group": {
                "_id": "$MMSI",
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gte": MIN_RECORDS_PER_MMSI}}},
        {"$project": {"_id": 1}},
    ]


def build_filter_pipeline(mmsi_values):
    return [
        {"$match": {"MMSI": {"$in": mmsi_values}}},
        *build_validation_stages(),
        {
            "$project": {
                "_id": 0,
                "__mmsi_num": 0,
                "__lat_num": 0,
                "__lon_num": 0,
                "__rot_num": 0,
                "__sog_num": 0,
                "__cog_num": 0,
                "__heading_num": 0,
            }
        },
    ]


def get_valid_mmsi_values(source):
    cursor = source.aggregate(
        build_valid_mmsi_pipeline(),
        allowDiskUse=True,
        batchSize=BATCH_SIZE,
    )
    valid_mmsi_values = [item["_id"] for item in cursor]
    print(f"Vessels with at least {MIN_RECORDS_PER_MMSI} valid points: {len(valid_mmsi_values)}")
    return valid_mmsi_values


def chunk_values(values, chunk_count):
    return [values[index::chunk_count] for index in range(chunk_count)]


def iter_query_chunks(values):
    for index in range(0, len(values), MMSI_QUERY_CHUNK_SIZE):
        yield values[index:index + MMSI_QUERY_CHUNK_SIZE]


def insert_filtered_records(source, filtered, mmsi_values, worker_id=None):
    batch = []
    inserted_count = 0

    for query_mmsi_values in iter_query_chunks(mmsi_values):
        cursor = source.aggregate(
            build_filter_pipeline(query_mmsi_values),
            allowDiskUse=True,
            batchSize=BATCH_SIZE,
        )

        for record in cursor:
            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                filtered.insert_many(batch, ordered=False)
                inserted_count += len(batch)
                if worker_id is None:
                    print(f"Inserted: {inserted_count}")
                else:
                    print(f"Worker {worker_id}: inserted {inserted_count}")
                batch = []

    if batch:
        filtered.insert_many(batch, ordered=False)
        inserted_count += len(batch)

    return inserted_count


def worker(worker_id, mmsi_values):
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    source = db[MAIN_COLLECTION]
    filtered = db[FILTERED_COLLECTION]

    try:
        return insert_filtered_records(
            source,
            filtered,
            mmsi_values,
            worker_id=worker_id,
        )
    finally:
        client.close()


def insert_filtered_records_parallel(valid_mmsi_values):
    
    total_inserted = 0

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [
            executor.submit(worker, worker_id, mmsi_chunk)
            for worker_id, mmsi_chunk in enumerate(chunk_values(valid_mmsi_values, NUM_WORKERS))
        ]

        for future in as_completed(futures):
            total_inserted += future.result()

    return total_inserted


def main():

    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    source = db[MAIN_COLLECTION]
    filtered = db[FILTERED_COLLECTION]

    valid_mmsi_values = get_valid_mmsi_values(source)

    print("Filtering and inserting started")
    total_inserted = insert_filtered_records_parallel(valid_mmsi_values)

    client.close()

    print(f"Total inserted: {total_inserted}")


if __name__ == "__main__":
    main()
