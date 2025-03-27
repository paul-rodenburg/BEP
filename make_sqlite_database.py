import sqlite3
from data_to_sql import process_data
from create_database_without_filter import process_data_without_filter
import os
from general import check_files

os.makedirs('databases', exist_ok=True)

check_files()
all_subset_input = input('Do you want to create a database from all lines in a ndjson file or from subsets? all [a] subsets [s]')
if all_subset_input == 'a':
    conn = sqlite3.connect('databases/reddit_data_ALL.db')
    process_data_without_filter(conn)
elif all_subset_input == 's':
    conn = sqlite3.connect('databases/reddit_data.db')
    process_data(conn)
else:
    print(f'{all_subset_input} is not a valid option. Please try again.')
    exit(0)
