import json
import pymongo
from config import *
from tqdm import tqdm
from general import get_primary_key
from line_counts import get_line_count_file
from concurrent.futures import ThreadPoolExecutor, TimeoutError

def get_collection_count(collection_to_count):
    """
    Gets the collection count with a timeout of 5 seconds.

    :param collection_to_count: MongoDB collection
    :return: docs count in collection or None if timeout occurs or an error happens
    """
    def count_docs():
        return collection_to_count.count_documents({})

    try:
        # Create a thread pool to run the count_docs function with a timeout
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(count_docs)  # Start counting in a separate thread
            count_result = future.result(timeout=5)  # Wait for the result with a 5-second timeout
            return count_result
    except TimeoutError:
        print(f"Timeout: Counting documents in collection '{collection_to_count.name}' took too long.")
        return None
    except Exception as e:
        print(f"Error while getting count for collection {collection_to_count.name}: {e}")
        return None

if __name__ == "__main__":
    # MongoDB Connection
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["reddit_data"]  # Database Name

    for data_file, collection_name in data_files_tables_mongo.items():
        collection = db[collection_name]  # Collection Name

        # Check if collection exists
        if collection_name in db.list_collection_names():
            count = get_collection_count(collection)

            if count is not None:
                response = input(f"Collection '{collection_name}' already exists with {count} documents. Remove it? (y/n): ")
            else:
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
        pm = get_primary_key(collection_name)
        collection.create_index([(pm, pymongo.ASCENDING)])

        # Open NDJSON file and insert in chunks
        with open(data_file, "r", encoding="utf-8") as file:
            buffer = []
            total_lines = get_line_count_file(posts_2025_1_file)

            with tqdm(total=total_lines, desc=f"Importing {collection_name} data to MongoDB collection {collection_name}", unit="docs") as pbar:
                for line in file:
                    if line.strip():  # Ignore empty lines
                        buffer.append(json.loads(line))

                    if len(buffer) >= CHUNK_SIZE:  # Insert when buffer reaches chunk size
                        collection.insert_many(buffer)
                        pbar.update(len(buffer))
                        buffer.clear()  # Clear buffer after inserting

                # Insert any remaining documents
                if buffer:
                    collection.insert_many(buffer)
                    pbar.update(len(buffer))

    print("Data import completed successfully!")

    # Close MongoDB connection
    client.close()