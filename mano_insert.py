import csv
from pymongo import MongoClient

FILE = "aisdk-2026-04-18.csv"
MAX_ROWS = 1000000

client = MongoClient("mongodb://localhost:27017")
db = client["ais_db"]
col = db["raw_data"]

def get_val(row, keys):
    for k in keys:
        if k in row:
            return row[k]
    return None

def main():
    print("STARTING INSERT")

    batch = []
    count = 0

    with open(FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                lat = get_val(row, ["Latitude", "LAT", "lat"])
                lon = get_val(row, ["Longitude", "LON", "lon"])
                mmsi = get_val(row, ["MMSI", "mmsi"])
                time = get_val(row, ["BaseDateTime", "time", "timestamp"])

                if not (lat and lon and mmsi):
                    continue

                doc = {
                    "MMSI": mmsi,
                    "Latitude": float(lat),
                    "Longitude": float(lon),
                    "# Timestamp": time
                }

                batch.append(doc)
                count += 1

            except:
                continue

            if len(batch) >= 5000:
                col.insert_many(batch)
                batch = []

            if count >= MAX_ROWS:
                break

            if count % 100000 == 0:
                print("Inserted:", count)

    if batch:
        col.insert_many(batch)

    print("DONE. TOTAL INSERTED:", count)

if __name__ == "__main__":
    main()
