import csv 
from pymongo import MongoClient
import threading
import queue
from concurrent.futures import ThreadPoolExecutor

#variables:

CSV_FILE = "/Users/ziviletareilyte/MongoDb_Test/aisdk-2026-04-18.csv"

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"

DATABASE_NAME = "main-db"
COLLECTION_NAME = "AIS_information"
QUEUE_MAX_SIZE = 2
BATCH_SIZE = 100000
MAX_WORKERS = 2
STOP_SIGNAL = None


def read_csv_to_queue(batch_queue):
    print("reader thread started")
    with open(CSV_FILE, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        batch = []
        batch_number = 0

        for row in reader:
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                batch_number += 1
                print(f"Read batch nr:{batch_number}")

                # If queue is full, reader waits here
                batch_queue.put(batch)

                batch = []

        if batch:
            batch_number += 1
            print(f"[READ] Batch {batch_number}")
            batch_queue.put(batch)

    # Tell all workers there is no more data
    for _ in range(MAX_WORKERS):
        batch_queue.put(STOP_SIGNAL)


def insert_worker(batch_queue, worker_id):
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    collection = client[DATABASE_NAME][COLLECTION_NAME]

    while True:
        batch = batch_queue.get()

        if batch is STOP_SIGNAL:
            batch_queue.task_done()
            break

        collection.insert_many(batch)
        batch_queue.task_done()

    client.close()


def main():
    print("Program started")
    batch_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)

    workers = []

    for i in range(MAX_WORKERS):
        thread = threading.Thread(
            target=insert_worker,
            args=(batch_queue, i + 1)
        )
        thread.start()
        workers.append(thread)

    reader_thread = threading.Thread(
        target=read_csv_to_queue,
        args=(batch_queue,)
    )

    reader_thread.start()

    reader_thread.join()
    batch_queue.join()

    for worker in workers:
        worker.join()

    print("CSV import finished.")


if __name__ == "__main__":
    main()