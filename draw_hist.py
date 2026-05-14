from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"

DATABASE_NAME = "aisdb"
COLLECTION_NAME = "filtered_AIS_information"

def main():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    print("Loading values...")

    delta_records = collection.find(
        {
            "delta_t_ms": {"$gt": 0}
        },
        {
            "_id": 0,
            "delta_t_ms": 1
        }
    )

    delta_values = []

    for doc in delta_records:
        delta_values.append(doc["delta_t_ms"])

    # convert to seconds
    delta_seconds = pd.Series(delta_values) / 1000

    # remove outliers
    filtered_data = delta_seconds[delta_seconds <= 60]

    #plot histogram
    plt.figure(figsize=(12, 6))
    plt.hist(filtered_data, bins=60)
    plt.xlabel("Time Difference (seconds)")
    plt.ylabel("Count")
    plt.ticklabel_format(style='plain', axis='y')
    plt.title("Distribution of Time Differences")
    plt.grid(True)
    plt.show()
    
    client.close()


if __name__ == "__main__":
    main()
