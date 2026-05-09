from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"

DATABASE_NAME = "main-db"
COLLECTION_NAME = "filtered_AIS_information"

TIME_FIELD = "# Timestamp"


def main():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    print("Calculating delta_t_ms...")

    pipeline = [
        {
            "$set": {
                "__timestamp_dt": {
                    "$dateFromString": {
                        "dateString": f"${TIME_FIELD}",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {
            "$match": {
                "__timestamp_dt": {"$ne": None}
            }
        },
        {
            "$setWindowFields": {
                "partitionBy": "$MMSI",
                "sortBy": {
                    "__timestamp_dt": 1
                },
                "output": {
                    "__previous_timestamp": {
                        "$shift": {
                            "output": "$__timestamp_dt",
                            "by": -1
                        }
                    }
                }
            }
        },
        {
            "$set": {
                "delta_t_ms": {
                    "$subtract": [
                        "$__timestamp_dt",
                        "$__previous_timestamp"
                    ]
                }
            }
        },
        {
            "$unset": [
                "__timestamp_dt",
                "__previous_timestamp"
            ]
        },
        {
            "$merge": {
                "into": COLLECTION_NAME,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard"
            }
        }
    ]

    collection.aggregate(pipeline, allowDiskUse=True)

    print("Finished. delta_t_ms added to filtered collection.")

    client.close()


if __name__ == "__main__":
    main()