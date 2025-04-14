import json
from sqlalchemy import create_engine
from data_to_sql import generate_sql_database
import os
from general import check_files

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

os.makedirs('databases', exist_ok=True)

# Load config
with open('config.json', 'r', encoding='utf-8') as f:
    data = json.load(f)['sqlite']
    db_location_relative = data['db_location_relative']

check_files()

engine = create_engine(f'sqlite:///{db_location_relative}')
generate_sql_database(engine)
