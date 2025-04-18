from data_to_sql import generate_sql_database
import os
from general import check_files, make_sqlite_engine

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

os.makedirs('databases', exist_ok=True)

check_files()

engine = make_sqlite_engine()
generate_sql_database(engine)
