from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"

DATABASE_NAME = "main-db"
COLLECTION_NAME = "filtered_AIS_information"
TIME_FIELD = "# Timestamp"
TIME_FORMAT = "%d/%m/%Y %H:%M:%S"


def main():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    print("Loading values...")

    cursor = collection.find(
        {
            "delta_t_ms": {"$gt": 0}
        },
        {
            "_id": 0,
            "delta_t_ms": 1
        }
    )

    delta_values = [doc["delta_t_ms"] for doc in cursor]

    # convert to seconds
    delta_seconds = pd.Series(delta_values) / 1000

    # remove outliers
    filtered_data = delta_seconds[delta_seconds <= 60]

    #plot histogram
    plt.figure(figsize=(12, 6))
    plt.hist(filtered_data, bins=60)
    plt.xlabel("Seconds")
    plt.ylabel("Frequency")
    plt.ticklabel_format(style='plain', axis='y')
    plt.title("Frequency of Delta Values")
    plt.grid(True)
    plt.show()
    
    client.close()


if __name__ == "__main__":
    main()
