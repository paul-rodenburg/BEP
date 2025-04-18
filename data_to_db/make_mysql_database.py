from sqlalchemy import text
import os
from data_to_sql import generate_sql_database
from general import check_files, make_mysql_engine

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

os.makedirs('databases', exist_ok=True)

# Make engine
engine = make_mysql_engine()

check_files()

DB_NAME = "reddit_data_ALL"

# Create a new database
with engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
    conn.commit()

# Make engine again if the database needed to be created
engine = make_mysql_engine(DB_NAME)
generate_sql_database(engine)

