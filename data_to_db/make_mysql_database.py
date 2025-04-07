from sqlalchemy import create_engine, text
import os
from data_to_sql import generate_sql_database
from general import check_files
import json

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

os.makedirs('databases', exist_ok=True)

# Load credentials from config
with open('config.json', 'r', encoding='utf-8') as f:
    data = json.load(f)['mysql']
    host = data["host"]
    user = data["username"]
    password = data["password"]
    custom_engine_url = data["custom_engine_url"]

# Make engine url for database
if custom_engine_url is not None:
    engine_url = custom_engine_url
elif password is not None:
    engine_url = f"mysql+pymysql://{user}:{password}@{host}"
else:
    engine_url = f"mysql+pymysql://{user}@{host}"

engine = create_engine(engine_url)

check_files()

DB_NAME = "reddit_data_ALL"

# Create a new database
with engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
    conn.commit()

engine = create_engine(f'{engine_url}/{DB_NAME}')
generate_sql_database(engine)

