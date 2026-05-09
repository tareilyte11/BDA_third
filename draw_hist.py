from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"

DATABASE_NAME = "main-db"
COLLECTION_NAME = "filtered_AIS_information"


def main():
    client = MongoClient(MONGO_URI)

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    print("Loading delta_t_ms values...")

    cursor = collection.find(
        {
            "delta_t_ms": {
                "$gt": 0
            }
        },
        {
            "_id": 0,
            "delta_t_ms": 1
        }
    )

    delta_values = [doc["delta_t_ms"] for doc in cursor]

    print(f"Loaded {len(delta_values)} delta_t values")

    # convert ms -> seconds
    delta_seconds = pd.Series(delta_values) / 1000

    print("\nStatistics:")
    print(delta_seconds.describe())

    # remove extreme outliers for readable histogram
    histogram_data = delta_seconds[delta_seconds <= 3600]

    plt.figure(figsize=(12, 6))

    plt.hist(
        histogram_data,
        bins=100
    )

    plt.xlabel("Delta t (seconds)")
    plt.ylabel("Frequency")
    plt.title("Histogram of AIS Delta t Values")

    plt.grid(True)

    plt.savefig("delta_t_histogram.png", dpi=300)

    plt.show()

    print("\nHistogram saved:")
    print("delta_t_histogram.png")

    client.close()


if __name__ == "__main__":
    main()