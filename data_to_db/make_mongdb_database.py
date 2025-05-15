import orjson as json
import pymongo
from tqdm import tqdm
from general import check_files, make_mongodb_client, update_summary_log
from line_counts import get_line_count_file
import os
from data_to_db.data_to_sql import add_file_table_db_info, is_file_tables_added_db, get_primary_key, load_json, write_json
import sys
from classes.logger import Logger
import time
from datetime import datetime
from classes.DBType import DBType, DBTypes
from itertools import islice

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

# Make 'databases' folder for SQLite database and .json file containing info about each database
os.makedirs('databases', exist_ok=True)

# Check if necessary data files exist
check_files()
pbar = None

# Make db_type object for MongoDB database
db_type = DBType(DBTypes.MONGODB, name_suffix='1m', max_rows=1_000_000)

# Set up the logger
os.makedirs("logs/summaries", exist_ok=True)
time_now = time.time()
log_filename = f"logs/sql_{time_now}.txt"
summary_filename = f"logs/summaries/summary_mongodb.json"
sys.stdout = Logger(log_filename)

# Load config
data = load_json('config.json')
data_files_tables = data['data_files_tables']
if db_type.max_rows:
    maximum_rows_database = db_type.max_rows
else:
    maximum_rows_database = data['maximum_rows_database']
chunk_size = data['mongodb']['chunk_size']

db = make_mongodb_client(db_type)
db_info_file = f'databases/db_info_mongodb_{db_type.name_suffix}.json'

print(f'[{db_type.display_name}] Max rows: {maximum_rows_database:,}')

count = 0
for data_file, tables_file in data_files_tables.items():
    count += 1
    collection_name = tables_file['mongodb']
    if not is_file_tables_added_db(data_file, collection_name, db_info_file):
        print(f'[{db_type.display_name}] Skipping {collection_name}...')
        continue
    collection = db[collection_name]  # Collection Name
    # Check if collection exists
    if collection_name in db.list_collection_names():

        response = input(f"[{db_type.display_name}] Collection '{collection_name}' already exists. Remove it? (y/n): ")

        if response == "y":
            collection.drop()  # Remove collection
            print(f"[{db_type.display_name}] Collection '{collection_name}' deleted.")
        elif response == "n":
            print(f"[{db_type.display_name}] Skipping collection '{collection_name}'.")
            continue  # Skip to next iteration if user says no

    # Time measurements
    start_time = datetime.now()
    # Add index
    pm = get_primary_key(collection_name)

    # Open NDJSON file and insert in chunks
    with open(data_file, "r", encoding="utf-8") as file:
        buffer = []
        total_lines = min(get_line_count_file(data_file), maximum_rows_database)
        line_count = 0
        pbar = tqdm(total=total_lines, desc=f"[{db_type.display_name}] Importing {collection_name} data to MongoDB collection {collection_name} [{count}/{len(data_files_tables)}]", unit="docs")
        for line in islice(file, maximum_rows_database):
            line_count += 1
            pbar.update(1)
            if line.strip():  # Ignore empty lines
                buffer.append(json.loads(line))

            if len(buffer) >= chunk_size:  # Insert when buffer reaches chunk size
                collection.insert_many(buffer)
                buffer.clear()  # Clear buffer after inserting

            # if line_count >= maximum_rows_database:  # Stop if there are maximum_rows_database written to avoid a very very large db
            #     if buffer:
            #         collection.insert_many(buffer)
            #         pbar.update(len(buffer))
            #         buffer.clear()  # Clear memory
            #     break

        # Insert any remaining documents
        if buffer:
            collection.insert_many(buffer)
            pbar.update(len(buffer))

        pbar.close()

        # Creating index
        if isinstance(pm, list):
            for primary_key in pm:
                print(f"[{db_type.display_name}] Creating index for '{collection_name}' and pm: {primary_key}...")
                collection.create_index([(primary_key, pymongo.ASCENDING)])
        else:
            print(f"[{db_type.display_name}] Creating index for '{collection_name}' and pm: {pm}...")
            collection.create_index([(pm, pymongo.ASCENDING)])

        # Time measurements
        end_time = datetime.now()

        update_summary_log(db_type=db_type, data_file=data_file,
                           start_time=start_time, end_time=end_time,
                           line_count=line_count, total_lines=total_lines,
                           tables=None, chunk_size=chunk_size, sql_writes=None)


    add_file_table_db_info(data_file, collection_name, db_info_file)

# Save the tqdm bar (for timing)
if pbar:
    print(str(pbar))

print(f"[{db_type.display_name}] Data import completed successfully!")
