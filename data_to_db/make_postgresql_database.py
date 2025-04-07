from sqlalchemy import create_engine
import os
from data_to_sql import generate_sql_database
from general import check_files
import json

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

os.makedirs('databases', exist_ok=True)

# Load credentials
with open('config.json', 'r', encoding='utf-8') as f:
    data = json.load(f)['postgresql']
    host = data["host"]
    user = data["username"]
    password = data["password"]
    port = data["port"]
    db_name = data["db_name"]
    custom_engine_url = data["custom_engine_url"]


check_files()

if custom_engine_url is not None:
    engine = create_engine(custom_engine_url)
else:
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")

generate_sql_database(engine)

