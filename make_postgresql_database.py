from sqlalchemy import create_engine
from data_to_sql import process_data
import os
from create_database_without_filter import process_data_without_filter
from general import check_files

os.makedirs('databases', exist_ok=True)

# PostgreSQL connection setup
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "localhost"  # Change if using a remote database
DB_PORT = "5432"

check_files()
all_subset_input = input('Do you want to create a database from all lines in a ndjson file or from subsets? all [a] subsets [s]')
if all_subset_input == 'a':
    DB_NAME = "reddit_data_ALL"
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    process_data_without_filter(engine)
elif all_subset_input == 's':
    DB_NAME = "reddit_data"
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    process_data(engine)
else:
    print(f'{all_subset_input} is not a valid option. Please try again.')
    exit(0)
