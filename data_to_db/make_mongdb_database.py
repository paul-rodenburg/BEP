import json
import pymongo
from tqdm import tqdm
from general import get_primary_key, check_files, make_mongodb_engine
from line_counts import get_line_count_file
import os
from data_to_sql import add_file_table_db_info, is_file_tables_added_db

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

# Make 'databases' folder for SQLite database and .json file containing info about each database
os.makedirs('databases', exist_ok=True)

# Check if necessary data files exist
check_files()

# Load config
with open('config.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    data_files_tables = data['data_files_tables']
    maximum_rows_database = data['maximum_rows_database']
    chunk_size = data['mongodb']['chunk_size']


db = make_mongodb_engine()
db_info_file = 'databases/db_info_mongodb.json'

for data_file, tables_file in data_files_tables.items():
    collection_name = tables_file['mongodb']
    if not is_file_tables_added_db(data_file, collection_name, db_info_file):
        input(f'[MongoDB] Skipping {collection_name}...')
        continue
    collection = db[collection_name]  # Collection Name

    # Check if collection exists
    if collection_name in db.list_collection_names():

        response = input(f"[MongoDB] Collection '{collection_name}' already exists. Remove it? (y/n): ")

        if response == "y":
            collection.drop()  # Remove collection
            input(f"[MongoDB] Collection '{collection_name}' deleted.")
        elif response == "n":
            input(f"[MongoDB] Skipping collection '{collection_name}'.")
            continue  # Skip to next iteration if user says no

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

                if len(buffer) >= chunk_size:  # Insert when buffer reaches chunk size
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
    add_file_table_db_info(data_file, collection_name, db_info_file)
print("Data import completed successfully!")
