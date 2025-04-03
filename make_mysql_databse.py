from sqlalchemy import create_engine, text
from data_to_sql import process_data
import os
from create_database_without_filter import process_data_without_filter
from general import check_files

os.makedirs('databases', exist_ok=True)

# Connect to MySQL
host = 'localhost'
user = 'root'
engine = create_engine(f"mysql+pymysql://{user}@{host}")

check_files()
all_subset_input = input('Do you want to create a database from all lines in a ndjson file or from subsets? all [a] subsets [s]')
if all_subset_input == 'a':
    DB_NAME = "reddit_data_ALL"

    # Create a new database
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
        conn.commit()

    engine = create_engine(f"mysql+pymysql://{user}@{host}/{DB_NAME}")
    process_data_without_filter(engine)
elif all_subset_input == 's':
    DB_NAME = "reddit_data"

    # Create a new database
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
        conn.commit()
    engine = create_engine(f"mysql+pymysql://{user}@{host}/{DB_NAME}")
    process_data(engine)
else:
    print(f'{all_subset_input} is not a valid option. Please try again.')
    exit(0)
