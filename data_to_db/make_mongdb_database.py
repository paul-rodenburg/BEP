import json
import pymongo
from tqdm import tqdm
from general import get_primary_key
from line_counts import get_line_count_file
import os

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

os.makedirs('databases', exist_ok=True)

# Load credentials from config
with open('config.json', 'r', encoding='utf-8') as f:
    data = json.load(f)['mongodb']
    host = data["host"]
    port = data["port"]
    db_name = data["db_name"]
    custom_engine_url = data["custom_engine_url"]

# Load config
with open('config.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    data_files_tables = data['data_files_tables']
    maximum_rows_database = data['maximum_rows_database']

# MongoDB Connection
if custom_engine_url is not None:
    client = pymongo.MongoClient(custom_engine_url)
else:
    client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client[db_name]  # Database Name

for data_file, tables_file in data_files_tables.items():
    collection_name = tables_file['mongodb']
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
        total_lines = min(get_line_count_file(data_file), maximum_rows_database)
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

                if line_count >= maximum_rows_database:  # Stop if there are maximum_rows_database written to avoid a very very large db
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