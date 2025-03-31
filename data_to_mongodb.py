import json
import pymongo
from config import *
from tqdm import tqdm
from general import get_primary_key
from line_counts import get_line_count_file
from concurrent.futures import ThreadPoolExecutor, TimeoutError

if __name__ == "__main__":
    # MongoDB Connection
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["reddit_data"]  # Database Name

    for data_file, collection_name in data_files_tables_mongo.items():
        collection = db[collection_name]  # Collection Name

        # Check if collection exists
        if collection_name in db.list_collection_names():

            response = input(f"Collection '{collection_name}' already exists. Remove it? (y/n): ")

            if response == "y":
                collection.drop()  # Remove collection
                print(f"Collection '{collection_name}' deleted.")
            elif response == "n":
                print(f"Skipping collection '{collection_name}'.")
                continue  # Skip to next iteration if user says no

        # Configurable chunk size for batch insertion
        CHUNK_SIZE = 1000  # Adjust as needed

        # Add index
        pm = get_primary_key(collection_name)[0]
        collection.create_index([(pm, pymongo.ASCENDING)])

        # Open NDJSON file and insert in chunks
        with open(data_file, "r", encoding="utf-8") as file:
            buffer = []
            total_lines = min(get_line_count_file(data_file), LINES_SUBSET)
            line_count = 0
            with tqdm(total=total_lines, desc=f"Importing {collection_name} data to MongoDB collection {collection_name}", unit="docs") as pbar:
                for line in file:
                    line_count += 1
                    if line.strip():  # Ignore empty lines
                        buffer.append(json.loads(line))

                    if len(buffer) >= CHUNK_SIZE:  # Insert when buffer reaches chunk size
                        collection.insert_many(buffer)
                        pbar.update(len(buffer))
                        buffer.clear()  # Clear buffer after inserting

                    if line_count >= LINES_SUBSET:  # Stop if there are LINES_SUBSET written to avoid a very very large db
                        if buffer:
                            collection.insert_many(buffer)
                            pbar.update(len(buffer))
                            buffer.clear()  # Clear memory
                        break

                # Insert any remaining documents
                if buffer:
                    collection.insert_many(buffer)
                    pbar.update(len(buffer))

    print("Data import completed successfully!")

    # Close MongoDB connection
    client.close()