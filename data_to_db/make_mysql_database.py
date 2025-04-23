from sqlalchemy import text
import os
from data_to_sql import generate_sql_database, load_json
from general import check_files, make_mysql_engine

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

# Make 'databases' folder for SQLite database and .json file containing info about each database
os.makedirs('databases', exist_ok=True)

# Check if character count file exists
# This is necessary to change TEXT to LONGTEXT for some attributes in MySQL, because of long lengths of data
if not os.path.isfile('character_lengths.json'):
    raise FileNotFoundError("character_lengths.json not found. Please run the script 'count_characters_db.py' first.")

# Make engine
engine = make_mysql_engine()

# Check if necessary data files exist
check_files()

DB_NAME = load_json('config.json')['mysql']['db_name']

# Create a new database
with engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
    conn.commit()

# Make engine again if the database needed to be created
engine = make_mysql_engine(DB_NAME)
generate_sql_database(engine)

